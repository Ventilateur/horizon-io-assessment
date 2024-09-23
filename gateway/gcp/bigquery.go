package gcp

import (
	"context"
	"horizon-io/core/entity"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

type BigQueryClient struct {
	client  *bigquery.Client
	dataset string
	table   string
}

func NewBigQueryClient(client *bigquery.Client, dataset string, table string) *BigQueryClient {
	return &BigQueryClient{
		client:  client,
		dataset: dataset,
		table:   table,
	}
}

type Row struct {
	IngestionId          string     `bigquery:"ingestion_id"`
	Date                 civil.Date `bigquery:"date"`
	ProjectId            string     `bigquery:"project_id"`
	CurrencySymbol       string     `bigquery:"currency_symbol"`
	NumberOfTransactions int        `bigquery:"number_of_transactions"`
	CurrencyValueUSD     string     `bigquery:"currency_value_usd"`
}

//func (r *Row) Save() (map[string]bigquery.Value, string, error) {
//	return map[string]bigquery.Value{
//		"ingestion_id":           r.IngestionId,
//		"date":                   r.Date,
//		"project_id":             r.ProjectId,
//		"currency_symbol":        r.CurrencySymbol,
//		"number_of_transactions": r.NumberOfTransactions,
//		"currency_value_usd":     r.CurrencyValueUSD.String(),
//	}, bigquery.NoDedupeID, nil
//}

func (c *BigQueryClient) Insert(ctx context.Context, ingestionId string, aggregates map[entity.AggregationKey]entity.AggregationValue) error {
	var rows []Row
	for k, v := range aggregates {
		rows = append(rows, Row{
			IngestionId:          ingestionId,
			Date:                 civil.DateOf(k.Date),
			ProjectId:            k.ProjectId,
			CurrencySymbol:       k.CurrencySymbol,
			NumberOfTransactions: v.NumberOfTransactions,
			CurrencyValueUSD:     v.CurrencyValueDecimal.String(),
		})
	}

	return c.client.Dataset(c.dataset).Table(c.table).Inserter().Put(ctx, rows)
}
