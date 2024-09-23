package aggregate

import (
	"context"
	"horizon-io/core/aggregate"
	"horizon-io/gateway/gcp"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/spf13/cobra"
)

var (
	bucket         string
	pricesFileName string
	inputFile      string
	dataset        string
	table          string
	ingestionId    string
)

func init() {
	Aggregate.Flags().StringVar(&bucket, "bucket", "", "")
	Aggregate.Flags().StringVar(&pricesFileName, "prices-file-name", "", "")
	Aggregate.Flags().StringVar(&inputFile, "input-file", "", "")
	Aggregate.Flags().StringVar(&dataset, "dataset", "", "")
	Aggregate.Flags().StringVar(&table, "table", "", "")
	Aggregate.Flags().StringVar(&ingestionId, "ingestion-id", "", "")
}

var Aggregate = &cobra.Command{
	Use: "aggregate",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		sc, err := storage.NewClient(ctx)
		if err != nil {
			return err
		}
		storageClient := gcp.NewStorageClient(sc, bucket)

		bq, err := bigquery.NewClient(ctx, bigquery.DetectProjectID)
		if err != nil {
			return err
		}
		bigqueryClient := gcp.NewBigQueryClient(bq, dataset, table)

		aggregator := aggregate.NewAggregator(storageClient, bigqueryClient, inputFile, pricesFileName)

		return aggregator.Do(ctx, ingestionId)
	},
}
