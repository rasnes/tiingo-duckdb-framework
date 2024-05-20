package extract

import (
	"fmt"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/rasnes/tiingo-duckdb-framework/append-last-trading-day/config"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
)

type Client struct {
	HTTPClient      *retryablehttp.Client
	Logger          *slog.Logger
	TiingoFormat    string
	TiingoStartDate string
	TiingoColumns   string
	tiingoToken     string
}

func NewClient(config *config.Config, logger *slog.Logger) (*Client, error) {
	tiingoToken := os.Getenv("TIINGO_TOKEN")
	if tiingoToken == "" {
		return nil, fmt.Errorf("TIINGO_TOKEN env variable is not set")
	}

	client := &Client{
		HTTPClient:      retryablehttp.NewClient(),
		Logger:          logger,
		TiingoFormat:    config.Tiingo.Format,
		TiingoStartDate: config.Tiingo.StartDate,
		TiingoColumns:   config.Tiingo.Columns,
		tiingoToken:     tiingoToken,
	}

	client.HTTPClient.RetryWaitMin = config.Extract.Backoff.RetryWaitMin
	client.HTTPClient.RetryWaitMax = config.Extract.Backoff.RetryWaitMax
	client.HTTPClient.RetryMax = config.Extract.Backoff.RetryMax
	client.HTTPClient.Logger = logger

	return client, nil
}

// addTiingoConfigToURL adds the Tiingo token to the URL
func (c *Client) addTiingoConfigToURL(rawURL string, history bool) (string, error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL: %w", err)
	}

	query := parsedURL.Query()
	query.Set("token", c.tiingoToken)
	query.Set("format", c.TiingoFormat)
	if history {
		query.Set("startDate", c.TiingoStartDate)
		query.Set("columns", c.TiingoColumns)
	}
	parsedURL.RawQuery = query.Encode()

	return parsedURL.String(), nil
}

// GetSupportedTickers fetches the supported tickers from the Tiingo API and returns the zip file downloaded
func (c *Client) GetSupportedTickers() ([]byte, error) {
	url := "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"

	body, resp, err := c.Get(url)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch the `supported_tickers.zip` file, status: %s, body: %s", resp.Status, string(body))
	}

	return body, nil
}

// GetLastTradingDay fetches prices for all tickers on the last completed training day
func (c *Client) GetLastTradingDay() ([]byte, error) {
	url, err := c.addTiingoConfigToURL("https://api.tiingo.com/tiingo/daily/prices", false)
	if err != nil {
		return nil, err
	}

	body, resp, err := c.Get(url)
	if err != nil {
		return nil, err
	}
	fmt.Println(resp.StatusCode)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch the `prices` file, status: %s, body: %s", resp.Status, string(body))
	}

	return body, nil
}

func (c *Client) GetHistory(ticker string) ([]byte, error) {
	url, err := c.addTiingoConfigToURL(fmt.Sprintf("https://api.tiingo.com/tiingo/daily/%s/prices", ticker), true)
	if err != nil {
		return nil, err
	}

	body, resp, err := c.Get(url)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch the `prices` file for ticker %s, status: %s, body: %s", ticker, resp.Status, string(body))
	}

	return body, nil
}

// Get fetches the URL and returns the body and response
func (c *Client) Get(url string) (body []byte, resp *http.Response, err error) {
	resp, err = c.HTTPClient.Get(url)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	return body, resp, nil
}
