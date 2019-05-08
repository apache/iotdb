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

package main

import (
	"fmt"
	"log"
	"strconv"
	_ "testing"
	"tsfile/common/constant"
	"tsfile/encoding/decoder"
	"tsfile/timeseries/read"

	"tsfile/timeseries/read/reader/impl/basic"
)

func TestTsFileSequenceReader(strPath string) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Error:", err)
		}
	}()

	//file := "goout/output1.ts"
	f := new(read.TsFileSequenceReader)
	f.Open(strPath)
	defer f.Close()

	headerString := f.ReadHeadMagic()
	log.Println("Header string: " + headerString)

	tailerString := f.ReadTailMagic()
	log.Println("Tail string: " + tailerString)

	fileMetadata := f.ReadFileMetadata()
	log.Println("File version: " + strconv.Itoa(fileMetadata.GetCurrentVersion()))

	for f.HasNextRowGroup() {
		groupHeader := f.ReadRowGroupHeader()
		log.Println("row group: " + groupHeader.GetDevice() + ", chunk number: " + strconv.Itoa(int(groupHeader.GetNumberOfChunks())) + ", end posistion: " + strconv.FormatInt(f.Pos(), 10))
		for i := 0; i < int(groupHeader.GetNumberOfChunks()); i++ {
			chunkHeader := f.ReadChunkHeader()
			log.Println("  chunk: " + chunkHeader.GetSensor() + ", page number: " + strconv.Itoa(chunkHeader.GetNumberOfPages()) + ", end posistion: " + strconv.FormatInt(f.Pos(), 10))
			defaultTimeDecoder := decoder.CreateDecoder(constant.TS_2DIFF, constant.INT64)
			valueDecoder := decoder.CreateDecoder(chunkHeader.GetEncodingType(), chunkHeader.GetDataType())
			for j := 0; j < chunkHeader.GetNumberOfPages(); j++ {
				pageHeader := f.ReadPageHeader(chunkHeader.GetDataType())
				log.Println("    page dps: " + strconv.Itoa(int(pageHeader.GetNumberOfValues())) + ", page data size: " + strconv.Itoa(int(pageHeader.GetCompressedSize())) + ", end posistion: " + strconv.FormatInt(f.Pos(), 10))

				pageData := f.ReadPage(pageHeader, chunkHeader.GetCompressionType())
				reader1 := &basic.PageDataReader{DataType: chunkHeader.GetDataType(), ValueDecoder: valueDecoder, TimeDecoder: defaultTimeDecoder}
				reader1.Read(pageData)
				for reader1.HasNext() {
					pair, _ := reader1.Next()
					log.Println("      (time,value): " + strconv.FormatInt(pair.Timestamp, 10) + ", " + fmt.Sprintf("%v", pair.Value))
				}
			}
		}
	}
}
