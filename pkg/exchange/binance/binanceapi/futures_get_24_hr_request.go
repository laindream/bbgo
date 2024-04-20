package binanceapi

import (
	"github.com/c9s/requestgen"
)

type FuturesGet24hrResponse struct {
	Symbol             string `json:"symbol"`
	PriceChange        string `json:"priceChange"`
	PriceChangePercent string `json:"priceChangePercent"`
	WeightedAvgPrice   string `json:"weightedAvgPrice"`
	LastPrice          string `json:"lastPrice"`
	LastQty            string `json:"lastQty"`
	OpenPrice          string `json:"openPrice"`
	HighPrice          string `json:"highPrice"`
	LowPrice           string `json:"lowPrice"`
	Volume             string `json:"volume"`
	QuoteVolume        string `json:"quoteVolume"`
	OpenTime           int64  `json:"openTime"`
	CloseTime          int64  `json:"closeTime"`
	FirstId            int    `json:"firstId"`
	LastId             int    `json:"lastId"`
	Count              int    `json:"count"`
}

//go:generate requestgen -method GET -url "/fapi/v1/ticker/24hr" -type FuturesGet24hrRequest -responseType .FuturesGet24hrResponse
type FuturesGet24hrRequest struct {
	client requestgen.APIClient

	symbol string `param:"symbol"`
}

func (c *RestClient) NewGetFutures24hrRequest() *FuturesGet24hrRequest {
	return &FuturesGet24hrRequest{client: c}
}
