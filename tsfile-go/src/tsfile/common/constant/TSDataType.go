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

package constant

type TSDataType int8

const (
	BOOLEAN TSDataType = 0
	INT32   TSDataType = 1
	INT64   TSDataType = 2
	FLOAT   TSDataType = 3
	DOUBLE  TSDataType = 4
	TEXT    TSDataType = 5
	INVALID TSDataType = -1
)

const (
	BOOLEAN_LEN int = 1
	SHORT_LEN   int = 2
	INT_LEN     int = 4
	LONG_LEN    int = 8
	FLOAT_LEN   int = 4
	DOUBLE_LEN  int = 8
)
