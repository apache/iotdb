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
	"github.com/cespare/xxhash"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/yanhongwangg/incubator-iotdb/client-go/src/main/client"
	"github.com/yanhongwangg/incubator-iotdb/client-go/src/main/client/utils"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
)

type conf struct {
	Host   string
	Port   string
	Sg     int32
}

var (
	session        = client.NewSession("127.0.0.1", "6667")
	deviceId       = "root.system_p.label_info"
	metricTagOrder = make(map[string]map[string]int32)
	timestamp      int64
	c              conf
)


func transSchema(tagKeyValues map[string]string, metric string, sgName string) (dvId string, sensor string) {
	paths := make(map[int32]string)
	tagOrderMap := metricTagOrder[metric]
	if tagOrderMap == nil {
		// it is a new metric
		tagOrderMap = make(map[string]int32)
	}
    for k, v := range tagKeyValues {
		tagOrder := tagOrderMap[k]
		if tagOrder == 0 {
			// it is a new tag
			tagOrderMap[k] = int32(len(tagOrderMap) + 1)
			measurements := []string{"metric_name", "tag_name", "tag_order"}
			dataTypes := []int32{utils.TEXT, utils.TEXT, utils.INT32}
			values := []interface{}{metric, k, tagOrderMap[k]}
			session.InsertRecord(deviceId, measurements, dataTypes, values, timestamp)

			timestamp++
			paths[tagOrderMap[k]] = v
		} else {
			paths[tagOrder] = v
		}
	}

	metricTagOrder[metric] = tagOrderMap
	dvId = "root."  + sgName + "." + metric + "."

	for i := 1; i < len(paths); i++ {
		if paths[int32(i)] == "" {
			paths[int32(i)] = "ph"
		}
		if i == len(paths)-1 {
			dvId += paths[int32(i)]
		} else {
			dvId += paths[int32(i)] + "."
		}
	}
	sensor = paths[int32(len(paths))]
	return dvId, sensor
}

func recoverMap() {
	dataSet := session.ExecuteQueryStatement("select * from root.system_p.label_info")
	for {
		if dataSet.HasNext() {
			timestamp++
			record := dataSet.Next()
			metricName := string(record.Fields[0].GetBinaryV())
			tagName := string(record.Fields[1].GetBinaryV())
			tagValue := record.Fields[2].GetIntV()

			if metricTagOrder[metricName] == nil {
				metricTagOrder[metricName] = make(map[string]int32)
			}

			metricTagOrder[metricName][tagName] = tagValue
		} else {
			break
		}
	}
}

func getData(host string, port string, sg int32) {
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
				if label.Name == "__name__" {
					metricName = label.Value
				} else {
					tagKeyValues[label.Name] = label.Value
				}
			}
			sgNum := strconv.Itoa(int(xxhash.Sum64String(metricName)%(uint64(sg))))
			sgName := "system_p_sg" + sgNum

			dvId, sensor := transSchema(tagKeyValues, metricName, sgName)
			time := ts.Samples[0].Timestamp
			value := ts.Samples[0].Value
			session.InsertRecord(dvId, []string{sensor}, []int32{utils.DOUBLE}, []interface{}{value}, time)
		}
	})
	server.ListenAndServe()
}

func (c *conf) getConf() *conf {
	yamlFile, err := ioutil.ReadFile("conf.yaml")
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}

	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
	return c
}


func main() {
	session.Open(false, 0)
	recoverMap()
	c.getConf()
	getData(c.Host,c.Port,c.Sg)
	session.Close()
}
