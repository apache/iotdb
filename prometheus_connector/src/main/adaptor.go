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
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/yanhongwangg/incubator-iotdb/client-go/src/main/client"
	"github.com/yanhongwangg/incubator-iotdb/client-go/src/main/client/utils"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)
//order starts with 1

type adaptor struct {
	paths []string
	tagOrderMap map[string]int32

}

var (
	session = client.NewSession("127.0.0.1", "6667")
	deviceId = "root.system_p.label_info"
	metricTagOrder = make(map[string]map[string]int32)
	largestOrderMap = make(map[string]int32) //metric->largestOrder
)

func (a *adaptor) transSchema(tagKeys []string, metric string) string {
	a.tagOrderMap = metricTagOrder[metric]
	if a.tagOrderMap == nil {
		//it is a new metric
		println("me ",metric, " tag orderMap is null")
		timeUnix:=time.Now().Unix() * 1000
		session.InsertStringRecord(deviceId, []string{"metric_name"}, []string{metric},
			timeUnix)
		//then,
		a.tagOrderMap = make(map[string]int32)
	}
	for i := 0; i < len(tagKeys); i++ {
		tagOrder := a.tagOrderMap[tagKeys[i]]
		if tagOrder == 0 {
			// it is a new tag
			timeUnix:=time.Now().Unix() * 1000
			session.InsertStringRecord(deviceId, []string{"tag_name"},
				[]string{tagKeys[i]}, timeUnix)
			a.tagOrderMap[tagKeys[i]] = largestOrderMap[metric] + 1
			session.InsertRecord(deviceId, []string{"tag_order"}, []int32{utils.INT32},
				[]interface{}{largestOrderMap[metric] + 1}, timeUnix)
			largestOrderMap[metric]= largestOrderMap[metric] + 1
		} else {
			a.paths[tagOrder] = tagKeys[i]
		}
	}
	location := len(a.paths) - 1
	res := "root.sg."
	for i := 1; i <= location; i++ {
		if a.paths[i] == "" {
			a.paths[i] = "ph"
		}
		res += a.paths[i] + "."
	}
	return res + metric
}


func getData()  {
	http.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		addr := strings.Split(r.RemoteAddr, ":")
		println(addr)
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		r.Body.Close()
		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	})
}


func main()  {
	var a adaptor
	session.Open(false,0)
	println(a.transSchema([]string{"s1","s2","s3"},"wyh"))
	session.Close()
}
