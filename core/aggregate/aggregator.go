package aggregate

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"horizon-io/core/entity"
	"horizon-io/gateway/gcp"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

const (
	nbFields          = 16
	fieldDateIdx      = 1
	fieldProjectIdIdx = 3
	fieldPropsIdx     = 14
	fieldNumsIdx      = 15
)

type Aggregator struct {
	storageClient  *gcp.StorageClient
	bigqueryClient *gcp.BigQueryClient
	inputObject    string
	pricesFileName string

	aggregates map[entity.AggregationKey]entity.AggregationValue
	prices     map[time.Time]map[string]decimal.Decimal
}

func NewAggregator(storageClient *gcp.StorageClient, bigqueryClient *gcp.BigQueryClient, inputObject, pricesFileName string) *Aggregator {
	aggregates := map[entity.AggregationKey]entity.AggregationValue{}
	prices := map[time.Time]map[string]decimal.Decimal{}
	return &Aggregator{
		storageClient:  storageClient,
		bigqueryClient: bigqueryClient,
		inputObject:    inputObject,
		pricesFileName: pricesFileName,

		aggregates: aggregates,
		prices:     prices,
	}
}

func (a *Aggregator) Do(ctx context.Context, ingestionId string) error {
	transactions, err := a.fetchTransactions(ctx)
	if err != nil {
		return err
	}

	if err := a.aggregate(transactions); err != nil {
		return err
	}

	if err := a.calculatePrices(ctx); err != nil {
		return err
	}

	if err := a.push(ctx, ingestionId); err != nil {
		return err
	}

	return nil
}

func (a *Aggregator) aggregate(in chan *entity.Record) error {
	for record := range in {
		aggKey := record.AggregationKey
		accumulatedValue, ok := a.aggregates[aggKey]
		if !ok {
			accumulatedValue = entity.AggregationValue{
				CurrencyValueDecimal: decimal.NewFromInt(0),
				NumberOfTransactions: 0,
			}
		}
		accumulatedValue.CurrencyValueDecimal = accumulatedValue.CurrencyValueDecimal.Add(record.CurrencyValueDecimal)
		accumulatedValue.NumberOfTransactions += 1
		a.aggregates[aggKey] = accumulatedValue
	}

	return nil
}

func (a *Aggregator) calculatePrices(ctx context.Context) error {
	var err error
	for k, v := range a.aggregates {
		pricesDate, ok := a.prices[k.Date]
		if !ok {
			pricesDate, err = a.fetchPrices(ctx, k.Date)
			if err != nil {
				return err
			}
			a.prices[k.Date] = pricesDate
		}
		price := pricesDate[strings.ToLower(k.CurrencySymbol)]
		v.CurrencyValueDecimal = price.Mul(v.CurrencyValueDecimal)
		a.aggregates[k] = v
	}

	return nil
}

func (a *Aggregator) push(ctx context.Context, ingestionId string) error {
	return a.bigqueryClient.Insert(ctx, ingestionId, a.aggregates)
}

func (a *Aggregator) fetchTransactions(ctx context.Context) (chan *entity.Record, error) {
	reader, err := a.storageClient.Reader(ctx, a.inputObject)
	if err != nil {
		return nil, err
	}

	out := make(chan *entity.Record, 500)
	csvReader := csv.NewReader(reader)

	go func() {
		defer close(out)

		lineNb := 1
		for {
			line, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				slog.Error("failed to read line", "error", err)
				continue
			}

			if lineNb == 1 {
				lineNb++
				continue
			}

			record, err := extractRecord(line)
			if err != nil {
				slog.Error(
					fmt.Sprintf("failed to process record %q", strings.Join(line, ",")),
					"error", err,
				)
				continue
			}

			out <- record
		}
	}()

	return out, nil
}

func (a *Aggregator) fetchPrices(ctx context.Context, date time.Time) (map[string]decimal.Decimal, error) {
	reader, err := a.storageClient.Reader(ctx, fmt.Sprintf("%d/%d/%d/%s", date.Year(), date.Month(), date.Day(), a.pricesFileName))
	if err != nil {
		return nil, err
	}

	prices := map[string]decimal.Decimal{}
	csvReader := csv.NewReader(reader)
	for {
		line, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		price, err := decimal.NewFromString(line[1])
		if err != nil {
			return nil, err
		}

		prices[line[0]] = price
	}

	return prices, nil
}

type Props struct {
	CurrencySymbol string `json:"currencySymbol"`
}

type Nums struct {
	CurrencyValueDecimal string `json:"currencyValueDecimal"`
}

func extractRecord(line []string) (*entity.Record, error) {
	if len(line) != nbFields {
		return nil, fmt.Errorf("expected 16 fields, got %d", len(line))
	}

	ts, err := time.Parse(time.DateTime, line[fieldDateIdx])
	if err != nil {
		return nil, err
	}
	date := time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, ts.Location())

	var props Props
	if err := json.Unmarshal([]byte(line[fieldPropsIdx]), &props); err != nil {
		return nil, err
	}

	var nums Nums
	if err := json.Unmarshal([]byte(line[fieldNumsIdx]), &nums); err != nil {
		return nil, err
	}

	value, err := decimal.NewFromString(nums.CurrencyValueDecimal)
	if err != nil {
		return nil, err
	}

	return &entity.Record{
		AggregationKey: entity.AggregationKey{
			Date:           date,
			ProjectId:      line[fieldProjectIdIdx],
			CurrencySymbol: strings.ToLower(props.CurrencySymbol),
		},
		CurrencyValueDecimal: value,
	}, nil
}
