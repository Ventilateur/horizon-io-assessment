package coingecko

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/shopspring/decimal"
)

const (
	baseUrl = "https://api.coingecko.com/api/v3"
)

type Client struct {
	httpClient *http.Client
	apiKey     string
}

func NewClient(httpClient *http.Client, apiKey string) *Client {
	return &Client{
		httpClient: httpClient,
		apiKey:     apiKey,
	}
}

type requestOpts struct {
	method  string
	url     string
	headers http.Header
	query   url.Values
	body    io.Reader
}

func sendRequest[Out any](httpClient *http.Client, opts requestOpts) (Out, error) {
	var out Out

	req, err := http.NewRequest(opts.method, opts.url, opts.body)
	if err != nil {
		return out, err
	}

	req.Header = opts.headers
	req.URL.RawQuery = opts.query.Encode()

	resp, err := httpClient.Do(req)
	if err != nil {
		return out, err
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return out, fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return out, err
	}

	return out, nil
}

func (c *Client) commonHeaders() http.Header {
	return http.Header{
		"accept":            []string{"application/json"},
		"x-cg-demo-api-key": []string{c.apiKey},
	}
}

type CoinWithPrice struct {
	Symbol       string          `json:"symbol"`
	CurrentPrice decimal.Decimal `json:"current_price"`
}

func (c *Client) ListCoinsWithPrices(vsCurrency string) ([]CoinWithPrice, error) {
	var result []CoinWithPrice
	for page := 1; ; page++ {
		prices, err := sendRequest[[]CoinWithPrice](c.httpClient, requestOpts{
			method:  http.MethodGet,
			url:     baseUrl + "/coins/markets",
			headers: c.commonHeaders(),
			query: url.Values{
				"vs_currency": []string{vsCurrency},
				"per_page":    []string{strconv.Itoa(250)},
				"page":        []string{strconv.Itoa(page)},
			},
		})
		if err != nil {
			return nil, err
		}

		if len(prices) == 0 {
			break
		}

		result = append(result, prices...)
	}

	return result, nil
}
