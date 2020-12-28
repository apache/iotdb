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
	"fmt"
	"github.com/apache/iotdb-client-go/client"
	"github.com/cespare/xxhash"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/yanhongwangg/incubator-iotdb/client-go/client/utils"
	"io/ioutil"
	"iotdb_prometheus/util"
	"net/http"
	"strconv"
)


func transReadSchema(tagKeyValues map[string]string, metric string, sgName string) (dvId string, sensor string) {
	paths := make(map[int32]string)
	tagOrderMap := util.MetricTagOrder[metric]
	for k, v := range tagKeyValues {
		tagOrder := tagOrderMap[k]
		if tagOrder == 0 {
			return "", ""
		} else {
			paths[tagOrder] = v
		}
	}
	dvId = "root."  + sgName + "." + metric
	if len(paths) == 0 {
		sensor = "*"
	} else {
		for i := 1; i <= len(paths)-1; i++ {
			if paths[int32(i)] == "" {
				paths[int32(i)] = "*"
			}
			dvId = dvId + "." + paths[int32(i)]
		}
		sensor = paths[int32(len(paths))]
	}
	return dvId, sensor
}

func readData(host string, port string, sgNum int32) *prompb.ReadResponse {
	server := http.Server{
		Addr: host + ":12333",
	}
	http.HandleFunc("/query", func (w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		reqBuf, _ := snappy.Decode(nil, compressed)
		var req prompb.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		for _, query := range req.Queries {
			var (
				tagKeyValues = make(map[string]string)
				metricName string
				dvId string
				sensor string
			)
			startTime := query.StartTimestampMs
			endTime := query.EndTimestampMs
			for _,label := range query.GetMatchers() {
				if label.Name == "__name__" {
					metricName = label.Value
				} else {
					tagKeyValues[label.Name] = label.Value
				}
			}
			if util.MetricTagOrder[metricName] == nil {
				continue
			} else {
				sgNum := strconv.Itoa(int(xxhash.Sum64String(metricName)%(uint64(sgNum))))
				sgName := "system_p_sg" + sgNum
				dvId, sensor = transReadSchema(tagKeyValues, metricName, sgName)
				if dvId == "" && sensor == "" {
					println(metricName, "dose not have information")
					continue
				}
			}
			sql := "select " + sensor + " from " + dvId + " where time >= " + strconv.Itoa(int(startTime)) + " and time <= " +
				strconv.Itoa(int(endTime))
			fmt.Println(sql)
			queryDataSet, _ := util.Session.ExecuteQueryStatement(sql)
			for i := 0; i < queryDataSet.GetColumnCount(); i++ {
				fmt.Printf("%s\t", queryDataSet.GetColumnName(i))
			}
			fmt.Println()
			for next, err := queryDataSet.Next(); err == nil && next; next, err = queryDataSet.Next() {
				if !queryDataSet.IsIgnoreTimeStamp() {
					fmt.Printf("%s\t", queryDataSet.GetText(client.TimestampColumnName))
				}
				for i := 0; i < queryDataSet.GetColumnCount(); i++ {
					columnName := queryDataSet.GetColumnName(i)
					v := queryDataSet.GetValue(columnName)
					if v == nil {
						v = "null"
					}
					fmt.Printf("%v\t\t", v)
				}
				fmt.Println()
			}
		}
		str:="aaa"
		w.Write([]byte(str))
		w.WriteHeader(http.StatusOK)
	})
	server.ListenAndServe()
	return nil
}

func transDataSet(queryDataSet *utils.SessionDataSet) *prompb.QueryResult {
	return nil
}

func main() {
	util.Session.Open(false, 0)
	util.RecoverMap()
	util.C.GetConf()
	readData(util.C.Host,util.C.Rport, util.C.Sg)
	util.Session.Close()
}

