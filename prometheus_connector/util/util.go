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

package util

import (
	"fmt"
	"github.com/apache/iotdb-client-go/client"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type conf struct {
	Host   string
	Wport  string
	Rport  string
	Sg     int32
}

var (
	config = &client.Config{
		Host:     "127.0.0.1",
		Port:     "6667",
		UserName: "root",
		Password: "root",
	}
	Session        = client.NewSession(config)
	DeviceId       = "root.system_p.label_info"
	MetricTagOrder = make(map[string]map[string]int32)
	Timestamp      int64
	C              conf
	MetricOrderTag = make(map[string]map[int32]string)
)

func RecoverMap() {
	dataSet, _ := Session.ExecuteQueryStatement("select * from root.system_p.label_info")
	fmt.Println()
	for {
		haseNext, _ := dataSet.Next()
		if haseNext {
			Timestamp++
			record, _ := dataSet.GetRowRecord()
			metricName := record.GetFields()[0].GetText()
			tagName := record.GetFields()[1].GetText()
			tagOrder := record.GetFields()[2].GetInt32()

			if MetricTagOrder[metricName] == nil {
				MetricTagOrder[metricName] = make(map[string]int32)
				MetricOrderTag[metricName] = make(map[int32]string)
			}

			MetricTagOrder[metricName][tagName] = tagOrder
			MetricOrderTag[metricName][tagOrder] = tagName
		} else {
			break
		}
	}
}

func (c *conf) GetConf() *conf {
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



