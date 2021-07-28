/*
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

// const in iotdb
const (
	SgPrefix = "system_p_sg"
	NodesSeparator = "."
	Wildcard = "*"
	Comma = " , "
	LastValue = "last_value"
	Ms = "ms"
	Root = "root"
	Select = "select "
	From = " from "
	LeftSmall = " ( "
	RightSmall = " ) "
	LeftMid = " [ "
	RightMid = " ] "
	GroupBy = " group by "
	Where = " where "
	Time = " time "
	Ge = " >= "
	Le = " <= "
	And = " and "
	// When there is no label in some positions,this position will be replaced by ph
	Placeholder = "ph"
	Fill = " fill(float[previous]) "
)

// const in prometheus
const (
	MetricKey = "__name__"
	StartTimeDeviation int64 = 300000
)

const (
	NullString = ""
)

