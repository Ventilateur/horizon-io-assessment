package price

import (
	"context"
	"errors"
	"horizon-io/core/price"
	"horizon-io/gateway/coingecko"
	"horizon-io/gateway/gcp"
	"os"

	"cloud.google.com/go/storage"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/spf13/cobra"
)

var (
	bucket         string
	pricesFileName string
)

func init() {
	FetchPrices.Flags().StringVar(&bucket, "bucket", "", "")
	FetchPrices.Flags().StringVar(&pricesFileName, "prices-file-name", "", "")
}

var FetchPrices = &cobra.Command{
	Use: "fetch-prices",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		retryClient := retryablehttp.NewClient()
		retryClient.Logger = nil
		httpClient := retryClient.StandardClient()

		apiKey := os.Getenv("COINGECKO_API_KEY")
		if apiKey == "" {
			return errors.New("required Coingecko API key in env var COINGECKO_API_KEY")
		}
		priceClient := coingecko.NewClient(httpClient, apiKey)

		sc, err := storage.NewClient(ctx)
		if err != nil {
			return err
		}

		storageClient := gcp.NewStorageClient(sc, bucket)

		fetcher := price.NewFetcher(priceClient, storageClient, pricesFileName)

		return fetcher.Do(ctx)
	},
}
