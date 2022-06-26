// Code generated by "requestgen -method GET -type Request -url /v1/metrics/:category/:metric -responseType DataSlice"; DO NOT EDIT.

package glassnodeapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"time"
)

func (r *Request) SetAsset(Asset string) *Request {
	r.Asset = Asset
	return r
}

func (r *Request) SetSince(Since time.Time) *Request {
	r.Since = &Since
	return r
}

func (r *Request) SetUntil(Until time.Time) *Request {
	r.Until = &Until
	return r
}

func (r *Request) SetInterval(Interval Interval) *Request {
	r.Interval = &Interval
	return r
}

func (r *Request) SetFormat(Format Format) *Request {
	r.Format = &Format
	return r
}

func (r *Request) SetCurrency(Currency string) *Request {
	r.Currency = &Currency
	return r
}

func (r *Request) SetTimestampFormat(TimestampFormat string) *Request {
	r.TimestampFormat = &TimestampFormat
	return r
}

func (r *Request) SetCategory(Category string) *Request {
	r.Category = Category
	return r
}

func (r *Request) SetMetric(Metric string) *Request {
	r.Metric = Metric
	return r
}

// GetQueryParameters builds and checks the query parameters and returns url.Values
func (r *Request) GetQueryParameters() (url.Values, error) {
	var params = map[string]interface{}{}
	// check Asset field -> json key a
	Asset := r.Asset

	// TEMPLATE check-required
	if len(Asset) == 0 {
		return nil, fmt.Errorf("a is required, empty string given")
	}
	// END TEMPLATE check-required

	// assign parameter of Asset
	params["a"] = Asset
	// check Since field -> json key s
	if r.Since != nil {
		Since := *r.Since

		// assign parameter of Since
		// convert time.Time to seconds time stamp
		params["s"] = strconv.FormatInt(Since.Unix(), 10)
	} else {
	}
	// check Until field -> json key u
	if r.Until != nil {
		Until := *r.Until

		// assign parameter of Until
		// convert time.Time to seconds time stamp
		params["u"] = strconv.FormatInt(Until.Unix(), 10)
	} else {
	}
	// check Interval field -> json key i
	if r.Interval != nil {
		Interval := *r.Interval

		// TEMPLATE check-valid-values
		switch Interval {
		case Interval1h, Interval24h, Interval10m, Interval1w, Interval1m:
			params["i"] = Interval

		default:
			return nil, fmt.Errorf("i value %v is invalid", Interval)

		}
		// END TEMPLATE check-valid-values

		// assign parameter of Interval
		params["i"] = Interval
	} else {
	}
	// check Format field -> json key f
	if r.Format != nil {
		Format := *r.Format

		// TEMPLATE check-valid-values
		switch Format {
		case FormatJSON, FormatCSV:
			params["f"] = Format

		default:
			return nil, fmt.Errorf("f value %v is invalid", Format)

		}
		// END TEMPLATE check-valid-values

		// assign parameter of Format
		params["f"] = Format
	} else {
		Format := "JSON"

		// assign parameter of Format
		params["f"] = Format
	}
	// check Currency field -> json key c
	if r.Currency != nil {
		Currency := *r.Currency

		// assign parameter of Currency
		params["c"] = Currency
	} else {
	}
	// check TimestampFormat field -> json key timestamp_format
	if r.TimestampFormat != nil {
		TimestampFormat := *r.TimestampFormat

		// assign parameter of TimestampFormat
		params["timestamp_format"] = TimestampFormat
	} else {
	}

	query := url.Values{}
	for _k, _v := range params {
		query.Add(_k, fmt.Sprintf("%v", _v))
	}

	return query, nil
}

// GetParameters builds and checks the parameters and return the result in a map object
func (r *Request) GetParameters() (map[string]interface{}, error) {
	var params = map[string]interface{}{}

	return params, nil
}

// GetParametersQuery converts the parameters from GetParameters into the url.Values format
func (r *Request) GetParametersQuery() (url.Values, error) {
	query := url.Values{}

	params, err := r.GetParameters()
	if err != nil {
		return query, err
	}

	for _k, _v := range params {
		if r.isVarSlice(_v) {
			r.iterateSlice(_v, func(it interface{}) {
				query.Add(_k+"[]", fmt.Sprintf("%v", it))
			})
		} else {
			query.Add(_k, fmt.Sprintf("%v", _v))
		}
	}

	return query, nil
}

// GetParametersJSON converts the parameters from GetParameters into the JSON format
func (r *Request) GetParametersJSON() ([]byte, error) {
	params, err := r.GetParameters()
	if err != nil {
		return nil, err
	}

	return json.Marshal(params)
}

// GetSlugParameters builds and checks the slug parameters and return the result in a map object
func (r *Request) GetSlugParameters() (map[string]interface{}, error) {
	var params = map[string]interface{}{}
	// check Category field -> json key category
	Category := r.Category

	// assign parameter of Category
	params["category"] = Category
	// check Metric field -> json key metric
	Metric := r.Metric

	// assign parameter of Metric
	params["metric"] = Metric

	return params, nil
}

func (r *Request) applySlugsToUrl(url string, slugs map[string]string) string {
	for _k, _v := range slugs {
		needleRE := regexp.MustCompile(":" + _k + "\\b")
		url = needleRE.ReplaceAllString(url, _v)
	}

	return url
}

func (r *Request) iterateSlice(slice interface{}, _f func(it interface{})) {
	sliceValue := reflect.ValueOf(slice)
	for _i := 0; _i < sliceValue.Len(); _i++ {
		it := sliceValue.Index(_i).Interface()
		_f(it)
	}
}

func (r *Request) isVarSlice(_v interface{}) bool {
	rt := reflect.TypeOf(_v)
	switch rt.Kind() {
	case reflect.Slice:
		return true
	}
	return false
}

func (r *Request) GetSlugsMap() (map[string]string, error) {
	slugs := map[string]string{}
	params, err := r.GetSlugParameters()
	if err != nil {
		return slugs, nil
	}

	for _k, _v := range params {
		slugs[_k] = fmt.Sprintf("%v", _v)
	}

	return slugs, nil
}

func (r *Request) Do(ctx context.Context) (DataSlice, error) {

	// no body params
	var params interface{}
	query, err := r.GetQueryParameters()
	if err != nil {
		return nil, err
	}

	apiURL := "/v1/metrics/:category/:metric"
	slugs, err := r.GetSlugsMap()
	if err != nil {
		return nil, err
	}

	apiURL = r.applySlugsToUrl(apiURL, slugs)

	req, err := r.Client.NewAuthenticatedRequest(ctx, "GET", apiURL, query, params)
	if err != nil {
		return nil, err
	}

	response, err := r.Client.SendRequest(req)
	if err != nil {
		return nil, err
	}

	var apiResponse DataSlice
	if err := response.DecodeJSON(&apiResponse); err != nil {
		return nil, err
	}
	return apiResponse, nil
}