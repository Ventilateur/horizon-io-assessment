package price

import (
	"context"
	"encoding/csv"
	"fmt"
	"horizon-io/gateway/coingecko"
	"horizon-io/gateway/gcp"
	"strings"
	"time"
)

const (
	vsCurrency = "usd"
)

type Fetcher struct {
	priceClient    *coingecko.Client
	storageClient  *gcp.StorageClient
	pricesFileName string
}

func NewFetcher(priceClient *coingecko.Client, storageClient *gcp.StorageClient, pricesFileName string) *Fetcher {
	return &Fetcher{
		priceClient:    priceClient,
		storageClient:  storageClient,
		pricesFileName: pricesFileName,
	}
}

func (f *Fetcher) Do(ctx context.Context) error {
	prices, err := f.priceClient.ListCoinsWithPrices(vsCurrency)
	if err != nil {
		return err
	}

	now := time.Now()
	object := fmt.Sprintf("%d/%d/%d/%s", now.Year(), now.Month(), now.Day(), f.pricesFileName)

	storageWriter := f.storageClient.Writer(ctx, object)
	defer storageWriter.Close()

	csvWriter := csv.NewWriter(storageWriter)
	for _, price := range prices {
		if err := csvWriter.Write([]string{
			strings.ToLower(price.Symbol),
			price.CurrentPrice.String(),
		}); err != nil {
			return err
		}
	}

	return nil
}
