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

func Foo() {
	fmt.Println("Foo")
}

type Client struct {
	HTTPClient   *retryablehttp.Client
	Logger       *slog.Logger
	TiingoFormat string
	tiingoToken  string
}

func NewClient(config *config.Config, logger *slog.Logger) (*Client, error) {
	tiingoToken := os.Getenv("TIINGO_TOKEN")
	if tiingoToken == "" {
		return nil, fmt.Errorf("TIINGO_TOKEN env variable is not set")
	}

	client := &Client{
		HTTPClient:   retryablehttp.NewClient(),
		Logger:       logger,
		TiingoFormat: config.Tiingo.Format,
		tiingoToken:  tiingoToken,
	}

	client.HTTPClient.RetryWaitMin = config.Extract.Backoff.RetryWaitMin
	client.HTTPClient.RetryWaitMax = config.Extract.Backoff.RetryWaitMax
	client.HTTPClient.RetryMax = config.Extract.Backoff.RetryMax
	client.HTTPClient.Logger = logger

	return client, nil
}

// AddTiingoConfigToURL adds the Tiingo token to the URL
func (c *Client) AddTiingoConfigToURL(rawURL string) (string, error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL: %w", err)
	}

	query := parsedURL.Query()
	query.Set("token", c.tiingoToken)
	query.Set("format", c.TiingoFormat)
	parsedURL.RawQuery = query.Encode()

	return parsedURL.String(), nil
}

// GetSupportedTickers fetches the supported tickers from the Tiingo API and returns the zip file downloaded
func (c *Client) GetSupportedTickers() ([]byte, error) {
	url := "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"
	resp, err := c.HTTPClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
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
	url, err := c.AddTiingoConfigToURL("https://api.tiingo.com/tiingo/daily/prices")
	if err != nil {
		return nil, err
	}

	resp, err := c.HTTPClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch the `prices` file, status: %s, body: %s", resp.Status, string(body))
	}

	return body, nil
}
