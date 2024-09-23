package aggregate

import (
	"horizon-io/core/entity"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestExtractRecord(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected *entity.Record
		hasErr   bool
	}{
		{
			name: "valid",
			input: []string{
				"seq-market", "2024-04-15 02:15:07.167", "BUY_ITEMS", "4974", "", "", "", "", "", "", "", "", "", "",
				`{"tokenId":"215","txnHash":"0xd919290e80df271e77d1cbca61f350d2727531e0334266671ec20d626b2104a2","chainId":"137","collectionAddress":"0x22d5f9b75c524fec1d6619787e582644cd4d7422","currencyAddress":"0xd1f9c58e33933a993a3891f8acfe05a68e1afc05","currencySymbol":"SFL","marketplaceType":"amm","requestId":""}`,
				`{"currencyValueDecimal":"0.6136203411678249","currencyValueRaw":"613620341167824900"}`,
			},
			expected: &entity.Record{
				AggregationKey: entity.AggregationKey{
					Date:           time.Date(2024, 4, 15, 0, 0, 0, 0, time.UTC),
					ProjectId:      "4974",
					CurrencySymbol: "sfl",
				},
				CurrencyValueDecimal: func() decimal.Decimal {
					v, err := decimal.NewFromString("0.6136203411678249")
					require.NoError(t, err)
					return v
				}(),
			},
		},
		{
			name:   "invalid number of fields",
			input:  []string{"", ""},
			hasErr: true,
		},
		{
			name: "invalid date",
			input: []string{
				"seq-market", "2024-04", "BUY_ITEMS", "4974", "", "", "", "", "", "", "", "", "", "",
				`{"tokenId":"215","txnHash":"0xd919290e80df271e77d1cbca61f350d2727531e0334266671ec20d626b2104a2","chainId":"137","collectionAddress":"0x22d5f9b75c524fec1d6619787e582644cd4d7422","currencyAddress":"0xd1f9c58e33933a993a3891f8acfe05a68e1afc05","currencySymbol":"SFL","marketplaceType":"amm","requestId":""}`,
				`{"currencyValueDecimal":"0.6136203411678249","currencyValueRaw":"613620341167824900"}`,
			},
			hasErr: true,
		},
		{
			name: "invalid json",
			input: []string{
				"seq-market", "2024-04-15 02:15:07.167", "BUY_ITEMS", "4974", "", "", "", "", "", "", "", "", "", "",
				`{{{{`,
				`{"currencyValueDecimal":"0.6136203411678249","currencyValueRaw":"613620341167824900"}`,
			},
			hasErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := extractRecord(tt.input)
			if tt.hasErr {
				require.NotNil(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestAggregate(t *testing.T) {
	tests := []struct {
		name     string
		input    []*entity.Record
		expected map[entity.AggregationKey]entity.AggregationValue
	}{
		{
			name: "valid",
			input: []*entity.Record{
				{
					AggregationKey: entity.AggregationKey{
						Date:           time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
						ProjectId:      "1",
						CurrencySymbol: "btc",
					},
					CurrencyValueDecimal: decimal.NewFromInt(1),
				},
				{
					AggregationKey: entity.AggregationKey{
						Date:           time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
						ProjectId:      "1",
						CurrencySymbol: "btc",
					},
					CurrencyValueDecimal: decimal.NewFromInt(2),
				},
				{
					AggregationKey: entity.AggregationKey{
						Date:           time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
						ProjectId:      "1",
						CurrencySymbol: "btc",
					},
					CurrencyValueDecimal: decimal.NewFromInt(3),
				},
				{
					AggregationKey: entity.AggregationKey{
						Date:           time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
						ProjectId:      "1",
						CurrencySymbol: "btc",
					},
					CurrencyValueDecimal: decimal.NewFromInt(5),
				},
				{
					AggregationKey: entity.AggregationKey{
						Date:           time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
						ProjectId:      "1",
						CurrencySymbol: "eth",
					},
					CurrencyValueDecimal: decimal.NewFromInt(2),
				},
				{
					AggregationKey: entity.AggregationKey{
						Date:           time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
						ProjectId:      "2",
						CurrencySymbol: "eth",
					},
					CurrencyValueDecimal: decimal.NewFromInt(2),
				},
			},
			expected: map[entity.AggregationKey]entity.AggregationValue{
				entity.AggregationKey{
					Date:           time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					ProjectId:      "1",
					CurrencySymbol: "btc",
				}: {
					CurrencyValueDecimal: decimal.NewFromInt(6),
					NumberOfTransactions: 3,
				},
				entity.AggregationKey{
					Date:           time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
					ProjectId:      "1",
					CurrencySymbol: "btc",
				}: {
					CurrencyValueDecimal: decimal.NewFromInt(5),
					NumberOfTransactions: 1,
				},
				entity.AggregationKey{
					Date:           time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
					ProjectId:      "1",
					CurrencySymbol: "eth",
				}: {
					CurrencyValueDecimal: decimal.NewFromInt(2),
					NumberOfTransactions: 1,
				},
				entity.AggregationKey{
					Date:           time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
					ProjectId:      "2",
					CurrencySymbol: "eth",
				}: {
					CurrencyValueDecimal: decimal.NewFromInt(2),
					NumberOfTransactions: 1,
				},
			},
		},
		{
			name:     "empty",
			input:    []*entity.Record{},
			expected: map[entity.AggregationKey]entity.AggregationValue{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := Aggregator{aggregates: map[entity.AggregationKey]entity.AggregationValue{}}

			c := make(chan *entity.Record, 10)
			go func() {
				defer close(c)
				for _, record := range tt.input {
					c <- record
				}
			}()

			err := a.aggregate(c)
			require.NoError(t, err)

			require.Equal(t, tt.expected, a.aggregates)
		})
	}
}
