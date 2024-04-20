package binanceapi

import (
	"github.com/c9s/requestgen"
)

type FuturesGetCommissionRateResponse struct {
	Symbol              string `json:"symbol"`
	MakerCommissionRate string `json:"makerCommissionRate"`
	TakerCommissionRate string `json:"takerCommissionRate"`
}

//go:generate requestgen -method GET -url "/fapi/v1/commissionRate" -type FuturesGetCommissionRateRequest -responseType .FuturesGetCommissionRateResponse
type FuturesGetCommissionRateRequest struct {
	client requestgen.APIClient

	symbol string `param:"symbol"`
}

func (c *RestClient) NewGetFuturesCommissionRateRequest() *FuturesGetCommissionRateRequest {
	return &FuturesGetCommissionRateRequest{client: c}
}
