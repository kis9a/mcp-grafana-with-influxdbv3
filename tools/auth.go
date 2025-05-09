package tools

import "net/http"

type authRoundTripper struct {
	accessToken string
	userToken   string
	apiKey      string
	underlying  http.RoundTripper
}

func (rt *authRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if rt.accessToken != "" && rt.userToken != "" {
		req.Header.Set("X-Access-Token", rt.accessToken)
		req.Header.Set("X-Grafana-Id", rt.userToken)
	} else if rt.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+rt.apiKey)
	}

	resp, err := rt.underlying.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
