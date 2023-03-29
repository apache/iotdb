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
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/backend/resource/httpadapter"
)

func iotdbResourceHandler(authorization string, httpClient *http.Client) backend.CallResourceHandler {
	mux := http.NewServeMux()

	mux.Handle("/getVariables", getVariables(authorization, httpClient))
	mux.Handle("/getNodes", getNodes(authorization, httpClient))

	return httpadapter.New(mux)
}

type queryReq struct {
	Sql string `json:"sql"`
}
type nodeReq struct {
	Data []string `json:"data"`
	Url  string   `json:"url"`
}

type queryResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func getVariables(authorization string, httpClient *http.Client) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		var url = r.FormValue("url")
		var sql = r.FormValue("sql")
		if r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		var queryReq = &queryReq{Sql: sql}
		qpJson, _ := json.Marshal(queryReq)
		reader := bytes.NewReader(qpJson)
		client := &http.Client{}
		request, _ := http.NewRequest(http.MethodPost, url+"/grafana/v1/variable", reader)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Add("Authorization", authorization)
		rsp, _ := client.Do(request)
		body, err := io.ReadAll(rsp.Body)
		if err != nil {
			log.DefaultLogger.Error("Data source is not working properly", err)
		}

		var dataResp []string
		err = json.Unmarshal(body, &dataResp)
		if err != nil {
			log.DefaultLogger.Error("Parsing JSON error", err)
			var resultResp queryResp
			json.Unmarshal(body, &resultResp)
			defer rsp.Body.Close()
			j, err := json.Marshal(resultResp)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_, err = w.Write(j)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

		} else {
			defer rsp.Body.Close()
			j, err := json.Marshal(dataResp)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_, err = w.Write(j)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

	}
	return http.HandlerFunc(fn)
}

func getNodes(authorization string, client *http.Client) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		s, _ := ioutil.ReadAll(r.Body)
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		var nodeReq nodeReq
		err := json.Unmarshal(s, &nodeReq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		qpJson, _ := json.Marshal(nodeReq.Data)
		reader := bytes.NewReader(qpJson)

		request, _ := http.NewRequest(http.MethodPost, nodeReq.Url+"/grafana/v1/node", reader)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Add("Authorization", authorization)
		rsp, _ := client.Do(request)
		body, err := io.ReadAll(rsp.Body)
		if err != nil {
			log.DefaultLogger.Error("Data source is not working properly", err)
		}

		var dataResp []string
		err = json.Unmarshal(body, &dataResp)
		if err != nil {
			log.DefaultLogger.Error("Parsing JSON error", err)
			var resultResp queryResp
			json.Unmarshal(body, &resultResp)
			defer rsp.Body.Close()
			j, err := json.Marshal(resultResp)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_, err = w.Write(j)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

		} else {
			defer rsp.Body.Close()
			j, err := json.Marshal(dataResp)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_, err = w.Write(j)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

	}
	return http.HandlerFunc(fn)
}
