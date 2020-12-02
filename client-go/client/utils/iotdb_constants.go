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

package utils

const (
	BOOLEAN int32 = 0
	INT32   int32 = 1
	INT64   int32 = 2
	FLOAT   int32 = 3
	DOUBLE  int32 = 4
	TEXT    int32 = 5
	INT     int32 = 6
)

const (
	PLAIN            int32 = 0
	PLAIN_DICTIONARY int32 = 1
	RLE              int32 = 2
	DIFF             int32 = 3
	TS_2DIFF         int32 = 4
	BITMAP           int32 = 5
	GORILLA_V1       int32 = 6
	REGULAR          int32 = 7
	GORILLA          int32 = 8
)

const (
	UNCOMPRESSED int32 = 0
	SNAPPY       int32 = 1
	GZIP         int32 = 2
	LZO          int32 = 3
	SDT          int32 = 4
	PAA          int32 = 5
	PLA          int32 = 6
	LZ4          int32 = 7
)
