// Code generated by "requestgen -method GET -url /fapi/v1/depth -type GetFuturesDepthRequest -responseType .Depth"; DO NOT EDIT.

package binanceapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"regexp"
)

func (g *GetFuturesDepthRequest) Symbol(symbol string) *GetFuturesDepthRequest {
	g.symbol = symbol
	return g
}

func (g *GetFuturesDepthRequest) Limit(limit int) *GetFuturesDepthRequest {
	g.limit = limit
	return g
}

// GetQueryParameters builds and checks the query parameters and returns url.Values
func (g *GetFuturesDepthRequest) GetQueryParameters() (url.Values, error) {
	var params = map[string]interface{}{}

	query := url.Values{}
	for _k, _v := range params {
		query.Add(_k, fmt.Sprintf("%v", _v))
	}

	return query, nil
}

// GetParameters builds and checks the parameters and return the result in a map object
func (g *GetFuturesDepthRequest) GetParameters() (map[string]interface{}, error) {
	var params = map[string]interface{}{}
	// check symbol field -> json key symbol
	symbol := g.symbol

	// assign parameter of symbol
	params["symbol"] = symbol
	// check limit field -> json key limit
	limit := g.limit

	// assign parameter of limit
	params["limit"] = limit

	return params, nil
}

// GetParametersQuery converts the parameters from GetParameters into the url.Values format
func (g *GetFuturesDepthRequest) GetParametersQuery() (url.Values, error) {
	query := url.Values{}

	params, err := g.GetParameters()
	if err != nil {
		return query, err
	}

	for _k, _v := range params {
		if g.isVarSlice(_v) {
			g.iterateSlice(_v, func(it interface{}) {
				query.Add(_k+"[]", fmt.Sprintf("%v", it))
			})
		} else {
			query.Add(_k, fmt.Sprintf("%v", _v))
		}
	}

	return query, nil
}

// GetParametersJSON converts the parameters from GetParameters into the JSON format
func (g *GetFuturesDepthRequest) GetParametersJSON() ([]byte, error) {
	params, err := g.GetParameters()
	if err != nil {
		return nil, err
	}

	return json.Marshal(params)
}

// GetSlugParameters builds and checks the slug parameters and return the result in a map object
func (g *GetFuturesDepthRequest) GetSlugParameters() (map[string]interface{}, error) {
	var params = map[string]interface{}{}

	return params, nil
}

func (g *GetFuturesDepthRequest) applySlugsToUrl(url string, slugs map[string]string) string {
	for _k, _v := range slugs {
		needleRE := regexp.MustCompile(":" + _k + "\\b")
		url = needleRE.ReplaceAllString(url, _v)
	}

	return url
}

func (g *GetFuturesDepthRequest) iterateSlice(slice interface{}, _f func(it interface{})) {
	sliceValue := reflect.ValueOf(slice)
	for _i := 0; _i < sliceValue.Len(); _i++ {
		it := sliceValue.Index(_i).Interface()
		_f(it)
	}
}

func (g *GetFuturesDepthRequest) isVarSlice(_v interface{}) bool {
	rt := reflect.TypeOf(_v)
	switch rt.Kind() {
	case reflect.Slice:
		return true
	}
	return false
}

func (g *GetFuturesDepthRequest) GetSlugsMap() (map[string]string, error) {
	slugs := map[string]string{}
	params, err := g.GetSlugParameters()
	if err != nil {
		return slugs, nil
	}

	for _k, _v := range params {
		slugs[_k] = fmt.Sprintf("%v", _v)
	}

	return slugs, nil
}

// GetPath returns the request path of the API
func (g *GetFuturesDepthRequest) GetPath() string {
	return "/fapi/v1/depth"
}

// Do generates the request object and send the request object to the API endpoint
func (g *GetFuturesDepthRequest) Do(ctx context.Context) (*Depth, error) {

	// empty params for GET operation
	var params interface{}
	query, err := g.GetParametersQuery()
	if err != nil {
		return nil, err
	}

	var apiURL string

	apiURL = g.GetPath()

	req, err := g.client.NewRequest(ctx, "GET", apiURL, query, params)
	if err != nil {
		return nil, err
	}

	response, err := g.client.SendRequest(req)
	if err != nil {
		return nil, err
	}

	var apiResponse Depth
	if err := response.DecodeJSON(&apiResponse); err != nil {
		return nil, err
	}

	type responseValidator interface {
		Validate() error
	}
	validator, ok := interface{}(apiResponse).(responseValidator)
	if ok {
		if err := validator.Validate(); err != nil {
			return nil, err
		}
	}
	return &apiResponse, nil
}
