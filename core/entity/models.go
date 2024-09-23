package entity

import (
	"time"

	"github.com/shopspring/decimal"
)

type AggregationKey struct {
	Date           time.Time
	ProjectId      string
	CurrencySymbol string
}

type AggregationValue struct {
	CurrencyValueDecimal decimal.Decimal
	NumberOfTransactions int
}

type Record struct {
	AggregationKey
	CurrencyValueDecimal decimal.Decimal
}
