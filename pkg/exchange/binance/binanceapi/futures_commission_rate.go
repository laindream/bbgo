package binanceapi

import (
	"github.com/c9s/requestgen"
)

type CommissionRateResponse struct {
	Symbol              string `json:"symbol"`
	MakerCommissionRate string `json:"makerCommissionRate"`
	TakerCommissionRate string `json:"takerCommissionRate"`
}

//go:generate requestgen -method GET -url "/fapi/v1/commissionRate" -type GetFuturesCommissionRateRequest -responseType .CommissionRateResponse
type GetFuturesCommissionRateRequest struct {
	client requestgen.APIClient

	symbol string `param:"symbol"`
}

func (c *RestClient) NewGetFuturesCommissionRateRequest() *GetFuturesCommissionRateRequest {
	return &GetFuturesCommissionRateRequest{client: c}
}
