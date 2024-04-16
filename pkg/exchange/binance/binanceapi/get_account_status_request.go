package binanceapi

import "github.com/c9s/requestgen"

type GetAccountStatusResponse struct {
	Data string `json:"data"`
}

//go:generate requestgen -method GET -url "/sapi/v1/account/status" -type GetAccountStatusRequest -responseType .GetAccountStatusResponse
type GetAccountStatusRequest struct {
	client requestgen.AuthenticatedAPIClient
}

func (c *RestClient) NewGetAccountStatusRequest() *GetAccountStatusRequest {
	return &GetAccountStatusRequest{client: c}
}
