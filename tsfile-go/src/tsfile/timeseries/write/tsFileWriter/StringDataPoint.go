/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tsFileWriter

import (
	"tsfile/common/constant"
)

/**
 * @Package Name: StringDataPoint
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-8-27 下午3:19
 * @Description:
 */

type StringDataPoint struct {
	sensorId string
	//tsDataType int16
	value string
}

func NewStringOld(sId string, tdt constant.TSDataType, val string) (*DataPoint, error) {
	return &DataPoint{
		sensorId: sId,
		//tsDataType: tdt,
		value: val,
	}, nil
}

func NewString(sId string, tdt constant.TSDataType, val string) (*DataPoint, error) {
	f := getDataPoint()
	f.sensorId = sId
	//f.tsDataType = tdt
	f.value = val
	return f, nil
}
