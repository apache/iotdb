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

package conf

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	CONFIG_FILE_NAME string = "tsfile-format.properties"

	MAGIC_STRING string = "TsFilev0.8.0"

	// Default bit width of RLE encoding is 8
	RLE_MIN_REPEATED_NUM   int = 8
	RLE_MAX_REPEATED_NUM   int = 0x7FFF
	RLE_MAX_BIT_PACKED_NUM int = 63

	// Gorilla encoding configuration
	FLOAT_LENGTH               int = 32
	FLAOT_LEADING_ZERO_LENGTH  int = 5
	FLOAT_VALUE_LENGTH         int = 6
	DOUBLE_LENGTH              int = 64
	DOUBLE_LEADING_ZERO_LENGTH int = 6
	DOUBLE_VALUE_LENGTH        int = 7
)

// Memory size threshold for flushing to disk or HDFS, default value is 128MB
var GroupSizeInByte int = 128 * 1024 * 1024

// The memory size for each series writer to pack page, default value is 64KB
var PageSizeInByte int = 64 * 1024

// The maximum number of data points in a page, defalut value is 1024 * 1024
var MaxNumberOfPointsInPage int = 1024 * 1024

// Data type for input timestamp, TsFile supports INT32 or INT64
var TimeSeriesDataType string = "INT64"

// Max length limitation of input string
var MaxStringLength int = 128

// Floating-point precision
var FloatPrecision int = 2

// Encoder of time series, TsFile supports TS_2DIFF, PLAIN and RLE(run-length encoding)
var TimeSeriesEncoder string = "TS_2DIFF"

// Encoder of value series. default value is PLAIN.
var ValueEncoder string = "PLAIN"

var Compressor string = "UNCOMPRESSED"

// Default block size of two-diff. delta encoding is 128
var DeltaBlockSize = 128

// Current version is 3
var CurrentVersion = 3

/**
* String encoder with UTF-8 encodes a character to at most 4 bytes.
 */
var BYTE_SIZE_PER_CHAR int = 4

func init() {
	loadProperties()
}

func loadProperties() {
	file, err := os.Open(CONFIG_FILE_NAME)
	defer file.Close()
	if err != nil {
		log.Println("Warn:", err)
		return
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if k, v := loadItem(scanner.Text()); v != "" {
			switch {
			case k == "group_size_in_byte":
				GroupSizeInByte, _ = strconv.Atoi(v)
			case k == "page_size_in_byte":
				PageSizeInByte, _ = strconv.Atoi(v)
			case k == "max_number_of_points_in_page":
				MaxNumberOfPointsInPage, _ = strconv.Atoi(v)
			case k == "time_series_data_type":
				TimeSeriesDataType = v
			case k == "max_string_length":
				MaxStringLength, _ = strconv.Atoi(v)
			case k == "float_precision":
				FloatPrecision, _ = strconv.Atoi(v)
			case k == "time_series_encoder":
				TimeSeriesEncoder = v
			case k == "value_encoder":
				ValueEncoder = v
			case k == "compressor":
				Compressor = v
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func loadItem(text string) (key string, value string) {
	text = strings.TrimSpace(text)
	if strings.HasPrefix(text, "#") {
		return "", ""
	} else if result := strings.Split(text, "="); len(result) > 1 {
		return strings.TrimSpace(result[0]), strings.TrimSpace(result[1])
	}

	return key, ""

}
