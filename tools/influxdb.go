package tools

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/DataDog/zstd"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	mcpgrafana "github.com/grafana/mcp-grafana"
	"github.com/mark3labs/mcp-go/server"
)

type dsQueryPayload struct {
	Queries []dsInnerQuery `json:"queries"`
	From    string         `json:"from"`
	To      string         `json:"to"`
}

type dsInnerQuery struct {
	RefID      string            `json:"refId"`
	Datasource map[string]string `json:"datasource"`
	Format     string            `json:"format"`
	RawSQL     string            `json:"rawSql"`
	RawQuery   bool              `json:"rawQuery"`
}

type dsQueryResponse struct {
	Results map[string]struct {
		Frames []struct {
			Schema any             `json:"schema"`
			Data   json.RawMessage `json:"data"`
		} `json:"frames"`
	} `json:"results"`
}

type influxdbClient struct {
	baseURL    string
	httpClient *http.Client
	uid        string
}

func newInfluxdbClient(ctx context.Context, uid string) (*influxdbClient, error) {
	if _, err := getDatasourceByUID(ctx, GetDatasourceByUIDParams{UID: uid}); err != nil {
		return nil, err
	}

	grafanaURL := strings.TrimRight(mcpgrafana.GrafanaURLFromContext(ctx), "/")
	base := fmt.Sprintf("%s/api/ds/query?ds_type=influxdb", grafanaURL)

	access, user := mcpgrafana.OnBehalfOfAuthFromContext(ctx)
	return &influxdbClient{
		baseURL: base,
		uid:     uid,
		httpClient: &http.Client{
			Transport: &authRoundTripper{
				accessToken: access,
				userToken:   user,
				apiKey:      mcpgrafana.GrafanaAPIKeyFromContext(ctx),
				underlying:  http.DefaultTransport,
			},
		},
	}, nil
}

func (c *influxdbClient) query(ctx context.Context, sql string) ([]map[string]any, error) {
	now := time.Now().UnixMilli()
	hrAgo := now - 60*60*1000

	payload := dsQueryPayload{
		From: fmt.Sprintf("%d", hrAgo),
		To:   fmt.Sprintf("%d", now),
		Queries: []dsInnerQuery{{
			RefID: "A",
			Datasource: map[string]string{
				"type": "influxdb",
				"uid":  c.uid,
			},
			Format:   "table",
			RawSQL:   sql,
			RawQuery: true,
		}},
	}

	b, _ := json.Marshal(payload)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request to Grafana /api/ds/query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		buf, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Grafana returned %d: %s", resp.StatusCode, buf)
	}

	var parsed dsQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode response JSON: %w", err)
	}

	ref := parsed.Results["A"]
	if len(ref.Frames) == 0 {
		return []map[string]any{}, nil
	}

	var dataStr string
	if err := json.Unmarshal(ref.Frames[0].Data, &dataStr); err == nil {
		decBase64, err := base64.StdEncoding.DecodeString(dataStr)
		if err != nil {
			return nil, fmt.Errorf("base64 decode frame: %w", err)
		}
		arrowBytes, err := zstd.Decompress(nil, decBase64)
		if err != nil {
			return nil, fmt.Errorf("zstd decompress: %w", err)
		}
		frames, err := data.UnmarshalArrowFrames([][]byte{arrowBytes})
		if err != nil {
			return nil, fmt.Errorf("unmarshal arrow frame: %w", err)
		}
		if len(frames) == 0 {
			return []map[string]any{}, nil
		}
		frame := frames[0]
		numRows := frame.Rows()
		records := make([]map[string]any, 0, numRows)
		for i := 0; i < numRows; i++ {
			row := make(map[string]any, len(frame.Fields))
			for _, f := range frame.Fields {
				row[f.Name] = f.At(i)
			}
			records = append(records, row)
		}
		return records, nil
	}

	var obj struct {
		Values [][]any `json:"values"`
	}
	if err := json.Unmarshal(ref.Frames[0].Data, &obj); err != nil {
		return nil, fmt.Errorf("unknown data format: %w", err)
	}
	return valuesMatrixToJSON(obj.Values, ref.Frames[0].Schema), nil
}

// Expand the column-oriented values array into row-oriented format
func valuesMatrixToJSON(vals [][]any, schema any) []map[string]any {
	if len(vals) == 0 || len(vals[0]) == 0 {
		return nil
	}
	rows := len(vals[0])
	cols := len(vals)
	var fieldNames []string
	if s, ok := schema.(map[string]any); ok {
		if flds, ok := s["fields"].([]any); ok {
			for _, f := range flds {
				if fm, ok := f.(map[string]any); ok {
					if name, ok := fm["name"].(string); ok {
						fieldNames = append(fieldNames, name)
					}
				}
			}
		}
	}
	out := make([]map[string]any, rows)
	for r := 0; r < rows; r++ {
		row := make(map[string]any, cols)
		for c := 0; c < cols; c++ {
			name := ""
			if c < len(fieldNames) {
				name = fieldNames[c]
			} else {
				name = fmt.Sprintf("col%d", c)
			}
			row[name] = vals[c][r]
		}
		out[r] = row
	}
	return out
}

type QueryInfluxSQLParams struct {
	DatasourceUID string `json:"datasourceUid" jsonschema:"required,description=InfluxDB v3 datasource UID"`
	SQL           string `json:"sql"           jsonschema:"required,description=SQL statement to execute"`
}

func queryInfluxSQL(ctx context.Context, args QueryInfluxSQLParams) ([]map[string]any, error) {
	cli, err := newInfluxdbClient(ctx, args.DatasourceUID)
	if err != nil {
		return nil, err
	}
	return cli.query(ctx, args.SQL)
}

var QueryInfluxSQL = mcpgrafana.MustTool(
	"query_influxdb_sql",
	"InfluxDB v3 datasource: Executes arbitrary SQL and returns the results as an array of JSON objects, one per row.",
	queryInfluxSQL,
)

func AddInfluxDBTools(mcp *server.MCPServer) {
	QueryInfluxSQL.Register(mcp)
}
