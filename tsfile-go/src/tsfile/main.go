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
	"os"
	"time"
	"tsfile/common/constant"
	"tsfile/common/log"
	"tsfile/timeseries/write/sensorDescriptor"
	"tsfile/timeseries/write/tsFileWriter"
)

const (
	fileName = "test.ts"
)

func main() {
	defer func() {
		if err := recover(); err != nil {
			log.Info("Error: ", err)
		}
	}()

	if _, err := os.Stat(fileName); !os.IsNotExist(err) {
		os.Remove(fileName)
	}

	// init tsFileWriter
	tfWriter, tfwErr := tsFileWriter.NewTsFileWriter(fileName)
	if tfwErr != nil {
		log.Info("init tsFileWriter error = %s", tfwErr)
	}

	// init sensorDescriptor
	sd, sdErr := sensorDescriptor.NewWithCompress("sensor_1", constant.TEXT, constant.PLAIN, constant.SNAPPY)
	if sdErr != nil {
		log.Info("init sensorDescriptor error = %s", sdErr)
	}
	//sd2, sdErr2 := sensorDescriptor.New("sensor_2", constant.TEXT, constant.PLAIN)
	//if sdErr2 != nil {
	//	log.Info("init sensorDescriptor error = %s", sdErr2)
	//}

	// add sensorDescriptor to tfFileWriter
	tfWriter.AddSensor(sd)
	//tfWriter.AddSensor(sd2)

	// create a tsRecord
	ts := time.Now()
	//timestamp := strconv.FormatInt(ts.UTC().UnixNano(), 10)
	//fmt.Println(timestamp)
	tr, trErr := tsFileWriter.NewTsRecord(ts, "device_1")
	if trErr != nil {
		log.Info("init tsRecord error.")
	}

	// create two data points
	fdp, fDpErr := tsFileWriter.NewString("sensor_1", constant.TEXT, "hello moto!")
	//fdp, fDpErr := tsFileWriter.NewDouble("sensor_1", constant.DOUBLE, 1.2)
	if fDpErr != nil {
		log.Info("init float data point error.")
	}
	//idp, iDpErr := tsFileWriter.NewString("sensor_2", constant.TEXT, "hello moto!")
	//if iDpErr != nil {
	//	log.Info("init int data point error.")
	//}

	// add data points to ts record
	tr.AddTuple(fdp)
	//tr.AddTuple(idp)

	// write tsRecord to file
	tfWriter.Write(tr)

	//log.Info("init tsRecord device_1_2")
	//
	//tr1, trErr1 := tsFileWriter.NewTsRecord(ts, "device_1")
	//if trErr1 != nil {
	//	log.Info("init tsRecord error.")
	//}
	//
	//// create two data points
	//fdp1, fDpErr1 := tsFileWriter.NewFloat("sensor_1", constant.FLOAT, 90.0)
	//if fDpErr1 != nil {
	//	log.Info("init float data point error.")
	//}
	//idp1, iDpErr1 := tsFileWriter.NewString("sensor_2", constant.TEXT, "hello lenovo!")
	//if iDpErr1 != nil {
	//	log.Info("init int data point error.")
	//}
	//
	//// add data points to ts record
	//tr1.AddTuple(fdp1)
	//tr1.AddTuple(idp1)
	//
	//// write tsRecord to file
	//tfWriter.Write(tr1)
	////tfWriter.Write([]byte("&TsFileData&"))
	//
	//log.Info("init tsRecord device_2.")
	//
	//ts3 := time.Now()
	//tr2, trErr2 := tsFileWriter.NewTsRecord(ts3, "lidong_2")
	//if trErr2 != nil {
	//	log.Info("init tsRecord error.")
	//}
	//
	//// create two data points
	//fdp2, fDpErr2 := tsFileWriter.NewFloat("sensor_1", constant.FLOAT, 1.2)
	//if fDpErr2 != nil {
	//	log.Info("init float data point error.")
	//}
	//idp2, iDpErr2 := tsFileWriter.NewString("sensor_2", constant.TEXT, "hello juzi!")
	//if iDpErr2 != nil {
	//	log.Info("init int data point error.")
	//}
	//
	//// add data points to ts record
	//tr2.AddTuple(fdp2)
	//tr2.AddTuple(idp2)
	//
	//// write tsRecord to file
	//tfWriter.Write(tr2)

	// close file descriptor
	tfWriter.Close()
}
