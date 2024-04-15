package binanceapi

import "github.com/c9s/requestgen"

//go:generate requestgen -method GET -url "/fapi/v1/depth" -type GetFuturesDepthRequest -responseType .Depth
type GetFuturesDepthRequest struct {
	client requestgen.APIClient

	symbol string `param:"symbol"`
	limit  int    `param:"limit" defaultValue:"1000"`
}

func (c *RestClient) NewGetFuturesDepthRequest() *GetFuturesDepthRequest {
	return &GetFuturesDepthRequest{client: c}
}
