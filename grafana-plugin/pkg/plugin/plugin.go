/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package plugin

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// Make sure IoTDBDatasource implements required interfaces. This is important to do
// since otherwise we will only get a not implemented error response from plugin in
// runtime. In this example datasource instance implements backend.QueryDataHandler,
// backend.CheckHealthHandler, backend.StreamHandler interfaces. Plugin should not
// implement all these interfaces - only those which are required for a particular task.
// For example if plugin does not need streaming functionality then you are free to remove
// methods that implement backend.StreamHandler. Implementing instancemgmt.InstanceDisposer
// is useful to clean up resources used by previous datasource instance when a new datasource
// instance created upon datasource settings changed.
var (
	_ backend.QueryDataHandler    = (*IoTDBDataSource)(nil)
	_ backend.CheckHealthHandler  = (*IoTDBDataSource)(nil)
	_ backend.CallResourceHandler = (*IoTDBDataSource)(nil)
)

// ApacheIoTDBDatasource creates a new datasource instance.
func ApacheIoTDBDatasource(d backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	var dm dataSourceModel
	if err := json.Unmarshal(d.JSONData, &dm); err != nil {
		return nil, err
	}
	ops, err := d.HTTPClientOptions()
	if err != nil {
		return nil, fmt.Errorf("http client options: %w", err)
	}
	httpClient, err := httpclient.New(ops)
	if err != nil {
		return nil, fmt.Errorf("new httpclient error: %w", err)
	}
	var authorization = ""
	if password, exists := d.DecryptedSecureJSONData["password"]; exists {
		authorization = "Basic " + base64.StdEncoding.EncodeToString([]byte(dm.Username+":"+password))
	}
	return &IoTDBDataSource{CallResourceHandler: iotdbResourceHandler(authorization, httpClient), Username: dm.Username, Ulr: dm.Url, httpClient: httpClient}, nil
}

// SampleDatasource is an example datasource which can respond to data queries, reports
// its health and has streaming skills.
type IoTDBDataSource struct {
	backend.CallResourceHandler
	Username   string
	Ulr        string
	httpClient *http.Client
}

// Dispose here tells plugin SDK that plugin wants to clean up resources when a new instance
// created. As soon as datasource settings change detected by SDK old datasource instance will
// be disposed and a new one will be created using ApacheIoTDBDatasource factory function.
func (d *IoTDBDataSource) Dispose() {
	// Clean up datasource instance resources.
	d.httpClient.CloseIdleConnections()
}

// QueryData handles multiple queries and returns multiple responses.
// req contains the queries []DataQuery (where each query contains RefID as a unique identifier).
// The QueryDataResponse contains a map of RefID to the response for each query, and each response
// contains Frames ([]*Frame).
func (d *IoTDBDataSource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	log.DefaultLogger.Info("QueryData called", "request", req)
	// create response struct
	response := backend.NewQueryDataResponse()

	// loop over queries and execute them individually.
	for _, q := range req.Queries {
		res := d.query(ctx, req.PluginContext, q)

		// save the response in a hashmap
		// based on with RefID as identifier
		response.Responses[q.RefID] = res
	}

	return response, nil
}

type dataSourceModel struct {
	Username string `json:"username"`
	Url      string `json:"url"`
}

type groupBy struct {
	GroupByLevel     string `json:"groupByLevel"`
	SamplingInterval string `json:"samplingInterval"`
	Step             string `json:"step"`
}

type queryParam struct {
	Expression   []string `json:"expression"`
	PrefixPath   []string `json:"prefixPath"`
	StartTime    int64    `json:"startTime"`
	EndTime      int64    `json:"endTime"`
	Condition    string   `json:"condition"`
	Control      string   `json:"control"`
	SqlType      string   `json:"sqlType"`
	Paths        []string `json:"paths"`
	AggregateFun string   `json:"aggregateFun"`
	FillClauses  string   `json:"fillClauses"`
	GroupBy      groupBy  `json:"groupBy"`
	Hide         bool     `json:"hide"`
}

type QueryDataReq struct {
	Expression []string `json:"expression"`
	PrefixPath []string `json:"prefix_path"`
	StartTime  int64    `json:"start_time"`
	EndTime    int64    `json:"end_time"`
	Condition  string   `json:"condition"`
	Control    string   `json:"control"`
}

type QueryDataResponse struct {
	Expressions []string        `json:"expressions"`
	Timestamps  []int64         `json:"timestamps"`
	Values      [][]interface{} `json:"values"`
	ColumnNames interface{}     `json:"column_names"`
	Code        int32           `json:"code"`
	Message     string          `json:"message"`
}

type loginStatus struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func NewQueryDataReq(expression []string, prefixPath []string, startTime int64, endTime int64, condition string, control string) *QueryDataReq {
	return &QueryDataReq{Expression: expression, PrefixPath: prefixPath, StartTime: startTime, EndTime: endTime, Condition: condition, Control: control}
}

func verifyQuery(query backend.DataQuery) (qp *queryParam, errMsg string) {

	err := json.Unmarshal(query.JSON, &qp)
	if err != nil {
		return nil, ""
	}

	if qp.Hide {
		return nil, "none"
	}

	if qp.SqlType == "SQL: Drop-down List" {
		if len(qp.Paths) < 1 {
			return nil, "Input error, please select TIME-SERIES"
		}
		if qp.GroupBy.SamplingInterval != "" && qp.AggregateFun == "" {
			return nil, "Input error, please select FUNCTION when SAMPLING INTERVAL has a value"
		}
	} else if qp.SqlType == "SQL: Full Customized" {
		if len(qp.Expression) == 0 {
			return nil, "Input error, SELECT is required"
		}

		for i := 0; i < len(qp.Expression); i++ {
			if qp.Expression[i] == "" {
				return nil, "Input error, SELECT is required"
			}

		}
		if len(qp.PrefixPath) == 0 {
			return nil, "Input error, FROM is required"
		}
		for i := 0; i < len(qp.PrefixPath); i++ {
			if qp.PrefixPath[i] == "" {
				return nil, "Input error, FROM is required"
			}
		}
	} else {
		return nil, "none"
	}

	return qp, ""
}

func (d *IoTDBDataSource) query(cxt context.Context, pCtx backend.PluginContext, query backend.DataQuery) backend.DataResponse {
	response := backend.DataResponse{}

	instanceSettings := pCtx.DataSourceInstanceSettings
	var authorization = ""
	if password, exists := instanceSettings.DecryptedSecureJSONData["password"]; exists {
		// Use the decrypted API key.
		authorization = "Basic " + base64.StdEncoding.EncodeToString([]byte(d.Username+":"+password))
	}

	// Unmarshal the JSON into our queryModel.

	var qdReq QueryDataReq
	qp, msg := verifyQuery(query)
	if msg == "none" {
		return response
	} else if msg != "" {
		response.Error = errors.New(msg)
		return response
	}

	qp.StartTime = query.TimeRange.From.UnixNano() / 1000000
	qp.EndTime = query.TimeRange.To.UnixNano() / 1000000

	if qp.SqlType == "SQL: Drop-down List" {
		qp.Control = ""
		var expressions []string = qp.Paths[len(qp.Paths)-1:]
		var paths []string = qp.Paths[0 : len(qp.Paths)-1]
		path := "root." + strings.Join(paths, ".")
		var prefixPaths = []string{path}
		if qp.AggregateFun != "" {
			expressions[0] = qp.AggregateFun + "(" + expressions[0] + ")"
		}
		if qp.GroupBy.SamplingInterval != "" && qp.GroupBy.Step == "" {
			qp.Control += " group by([" + strconv.FormatInt(qp.StartTime, 10) + "," + strconv.FormatInt(qp.EndTime, 10) + ")," + qp.GroupBy.SamplingInterval + ")"
		}
		if qp.GroupBy.SamplingInterval != "" && qp.GroupBy.Step != "" {
			qp.Control += " group by([" + strconv.FormatInt(qp.StartTime, 10) + "," + strconv.FormatInt(qp.EndTime, 10) + ")," + qp.GroupBy.SamplingInterval + "," + qp.GroupBy.Step + ")"
		}
		if qp.GroupBy.GroupByLevel != "" {
			qp.Control += " " + qp.GroupBy.GroupByLevel
		}
		if qp.FillClauses != "" {
			qp.Control += " fill" + qp.FillClauses
		}
		qdReq = *NewQueryDataReq(expressions, prefixPaths, qp.StartTime, qp.EndTime, qp.Condition, qp.Control)
	} else if qp.SqlType == "SQL: Full Customized" {
		qdReq = *NewQueryDataReq(qp.Expression, qp.PrefixPath, qp.StartTime, qp.EndTime, qp.Condition, qp.Control)
	} else {
		return response
	}
	qpJson, _ := json.Marshal(qdReq)
	reader := bytes.NewReader(qpJson)

	var dataSourceUrl = DataSourceUrlHandler(d.Ulr)

	request, _ := http.NewRequest(http.MethodPost, dataSourceUrl+"/grafana/v1/query/expression", reader)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Add("Authorization", authorization)

	rsp, _ := d.httpClient.Do(request)
	body, err := io.ReadAll(rsp.Body)
	if err != nil {
		response.Error = errors.New("Data source is not working properly")
		log.DefaultLogger.Error("Data source is not working properly", err)
		return response
	}

	var queryDataResp QueryDataResponse
	err = json.Unmarshal(body, &queryDataResp)
	if err != nil {
		response.Error = errors.New("Parsing JSON error")
		log.DefaultLogger.Error("Parsing JSON error", err)
		return response
	}

	defer rsp.Body.Close()
	if queryDataResp.Code > 0 {
		response.Error = errors.New(queryDataResp.Message)
		log.DefaultLogger.Error(queryDataResp.Message)
		return response

	}
	// create data frame response.
	frame := data.NewFrame("response")
	for i := 0; i < len(queryDataResp.Expressions); i++ {
		if queryDataResp.Timestamps == nil || len(queryDataResp.Timestamps) == 0 {
			times := make([]time.Time, 1)
			tmp := make([]float64, 1)
			frame.Fields = append(frame.Fields,
				data.NewField("time", nil, times),
				data.NewField(queryDataResp.Expressions[i], nil, tmp),
			)
			continue
		}
		times := make([]time.Time, len(queryDataResp.Timestamps))
		for c := 0; c < len(queryDataResp.Timestamps); c++ {
			times[c] = time.Unix(queryDataResp.Timestamps[c]/1000, 0)
		}
		values := recoverType(queryDataResp.Values[i])
		frame.Fields = append(frame.Fields,
			data.NewField("time", nil, times),
			data.NewField(queryDataResp.Expressions[i], nil, values),
		)
	}

	response.Frames = append(response.Frames, frame)
	return response
}

func recoverType(m []interface{}) interface{} {
	if len(m) > 0 {
		switch m[0].(type) {
		case float64:
			tmp := make([]float64, len(m))
			for i := range m {
				if m[i] == nil {
					tmp[i] = 0
				} else {
					tmp[i] = m[i].(float64)
				}
			}
			return tmp
		case string:
			tmp := make([]string, len(m))
			for i := range m {
				tmp[i] = m[i].(string)
			}
			return tmp
		case bool:
			tmp := make([]float64, len(m))
			for i := range m {
				if m[i].(bool) {
					tmp[i] = 1
				} else {
					tmp[i] = 0
				}
			}
			return tmp
		default:
			tmp := make([]float64, len(m))
			for i := range m {
				if m[i] == nil {
					tmp[i] = 0
				} else {
					tmp[i] = m[i].(float64)
				}
			}
			return tmp
		}
	} else {
		return make([]float64, 0)
	}
}

// Whether the last character of the URL for processing datasource configuration is "/"
func DataSourceUrlHandler(url string) string {
	var lastCharacter = url[len(url)-1:]
	if lastCharacter == "/" {
		url = url[0 : len(url)-1]
	}
	return url
}

// CheckHealth handles health checks sent from Grafana to the plugin.
// The main use case for these health checks is the test button on the
// datasource configuration page which allows users to verify that
// a datasource is working as expected.
func (d *IoTDBDataSource) CheckHealth(_ context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	log.DefaultLogger.Info("CheckHealth called", "request", req)

	var status = backend.HealthStatusOk
	var message = "Data source is working"

	loginStatus, err := d.IoTDBLogin(req)

	if err != nil {
		log.DefaultLogger.Error("Parsing JSON error", err)
		status = backend.HealthStatusError
		message = fmt.Sprint("Failed to connect to iotdb service.", err.Error())
	} else if loginStatus.Code != 200 {
		status = backend.HealthStatusError
		message = loginStatus.Message
	}
	return &backend.CheckHealthResult{
		Status:  status,
		Message: message,
	}, nil
}

func (d *IoTDBDataSource) IoTDBLogin(req *backend.CheckHealthRequest) (*loginStatus, error) {
	instanceSettings := req.PluginContext.DataSourceInstanceSettings
	var authorization = ""
	if password, exists := instanceSettings.DecryptedSecureJSONData["password"]; exists {
		authorization = "Basic " + base64.StdEncoding.EncodeToString([]byte(d.Username+":"+password))
	}
	var dataSourceUrl = DataSourceUrlHandler(d.Ulr)
	request, err := http.NewRequest(http.MethodGet, dataSourceUrl+"/grafana/v1/login", nil)
	if err != nil {
		log.DefaultLogger.Error("Error creating NewRequest", err.Error())
		return nil, err
	}
	request.Header.Add("Authorization", authorization)
	response, err := d.httpClient.Do(request)
	if err != nil {
		log.DefaultLogger.Error("Failed to connect to iotdb service", err.Error())
		return nil, err
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		log.DefaultLogger.Error("Failed to get iotdb service data", err.Error())
		return nil, err
	}
	var loginStatus loginStatus
	err = json.Unmarshal(body, &loginStatus)
	if err != nil {
		log.DefaultLogger.Error("Parsing JSON error", err)
		return nil, err
	}

	defer response.Body.Close()
	return &loginStatus, nil
}
