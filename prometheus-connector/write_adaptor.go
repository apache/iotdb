/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package main

import (
	"github.com/apache/iotdb-client-go/client"
	"github.com/cespare/xxhash"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"io/ioutil"
	"iotdb_prometheus/util"
	"net/http"
	"strconv"
	"strings"
)



func transWriteSchema(tagKeyValues map[string]string, metric string, sgName string) (dvId string, sensor string) {
	paths := make(map[int32]string)
	tagOrderMap := util.MetricTagOrder[metric]
	orderTagMap := util.MetricOrderTag[metric]
	if tagOrderMap == nil {
		// it is a new metric
		tagOrderMap = make(map[string]int32)
		orderTagMap = make(map[int32]string)
	}
    for k, v := range tagKeyValues {
		tagOrder := tagOrderMap[k]
		if tagOrder == 0 {
			// it is a new tag
			tagOrderMap[k] = int32(len(tagOrderMap) + 1)
			orderTagMap[int32(len(tagOrderMap) + 1)] = k
			measurements := []string{"metric_name", "tag_name", "tag_order"}
			dataTypes := []client.TSDataType{client.TEXT, client.TEXT, client.INT32}
			values := []interface{}{metric, k, tagOrderMap[k]}
			util.Session.InsertRecord(util.DeviceId, measurements, dataTypes, values, util.Timestamp)

			util.Timestamp++
			paths[tagOrderMap[k]] = v
		} else {
			paths[tagOrder] = v
		}
	}
	util.MetricTagOrder[metric] = tagOrderMap
	util.MetricOrderTag[metric] = orderTagMap
	dvId = "root."  + sgName + "." + metric

	for i := 1; i <= len(paths)-1; i++ {
		if paths[int32(i)] == "" {
			paths[int32(i)] = "ph"
		}
		dvId += "." + paths[int32(i)]
	}
	sensor = paths[int32(len(paths))]
	return dvId, sensor
}


func writeData(host string, port string, sgNum int32) {
	server := http.Server{
		Addr: host + ":" + port ,
	}

	http.HandleFunc("/receive", func (w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		compressed, _ := ioutil.ReadAll(r.Body)
		reqBuf, _ := snappy.Decode(nil, compressed)
		var req prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		for _, ts := range req.Timeseries {
			var (
				tagKeyValues = make(map[string]string)
				metricName string
			)
			for _ , label := range ts.Labels {
				label.Value = strings.Replace(label.Value, "\"","\\\"",-1)
				if label.Name == "__name__" {
					metricName = "\"" +label.Value + "\""
				} else {
					tagKeyValues[label.Name] = "\"" + label.Value + "\""
				}
			}
			sgNum := strconv.Itoa(int(xxhash.Sum64String(metricName)%(uint64(sgNum))))
			sgName := "system_p_sg" + sgNum
			dvId, sensor := transWriteSchema(tagKeyValues, metricName, sgName)
			time := ts.Samples[0].Timestamp
			value := ts.Samples[0].Value
			util.Session.InsertRecord(dvId, []string{sensor}, []client.TSDataType{client.DOUBLE}, []interface{}{value}, time)
		}
	})
	server.ListenAndServe()
}


func main() {
	util.Session.Open(false, 0)
	util.RecoverMap()
	util.C.GetConf()
	writeData(util.C.Host,util.C.Wport,util.C.Sg)
	util.Session.Close()
}
