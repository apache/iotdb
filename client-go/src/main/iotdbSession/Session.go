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

package iotdbSession

import (
	"context"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/yanhongwangg/go-thrift/rpc"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

var client *rpc.TSIServiceClient
var protocolVersion = rpc.TSProtocolVersion_IOTDB_SERVICE_PROTOCOL_V3
var sessionId int64
var isClose bool = true
var trans thrift.TTransport
var err error
var requestStatementId int64

func (session *Session) Open(enableRPCCompression bool, connectionTimeoutInMs int) error {
	dir, _ := os.Getwd()
	os.Mkdir(dir+"\\logs", os.ModePerm)
	logFile, logErr := os.OpenFile(dir+"\\logs\\all.log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	println(logErr)
	log.SetOutput(logFile)
	var protocolFactory thrift.TProtocolFactory
	trans, err = thrift.NewTSocketTimeout(net.JoinHostPort(session.Host, session.Port), time.Duration(connectionTimeoutInMs))
	if err != nil {
		log.Printf("connect error = %v", err)
		return err
	}
	trans = thrift.NewTFramedTransport(trans)
	if !trans.IsOpen() {
		err = trans.Open()
		if err != nil {
			log.Printf("open the conn failed %v", err)
			return err
		}
	}
	if enableRPCCompression {
		protocolFactory = thrift.NewTCompactProtocolFactory()
	} else {
		protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
	}
	iprot := protocolFactory.GetProtocol(trans)
	oprot := protocolFactory.GetProtocol(trans)
	client = rpc.NewTSIServiceClient(thrift.NewTStandardClient(iprot, oprot))
	session.ZoneId = DefaultZoneId
	tSOpenSessionReq := rpc.TSOpenSessionReq{ClientProtocol: protocolVersion, ZoneId: session.ZoneId, Username: &session.User, Password: &session.Passwd}
	tSOpenSessionResp, err := client.OpenSession(context.Background(), &tSOpenSessionReq)
	if err != nil {
		log.Printf("open session failed, err: %v", err)
		return err
	}
	sessionId = tSOpenSessionResp.GetSessionId()
	log.Printf("open session success, msg: %v", tSOpenSessionResp.GetStatus().Code)
	requestStatementId, err = client.RequestStatementId(context.Background(), sessionId)
	session.FetchSize = DefaultFetchSize
	if err != nil {
		log.Printf("request StatementId failed, err: %v", err)
		return err
	}
	if session.ZoneId != "" {
		session.SetTimeZone(session.ZoneId)
	} else {
		session.ZoneId = session.GetTimeZone()
	}
	return nil
}

func (session *Session) Close() error {
	tSCloseSessionReq := rpc.NewTSCloseSessionReq()
	tSCloseSessionReq.SessionId = sessionId
	status, err := client.CloseSession(context.Background(), tSCloseSessionReq)
	if err != nil {
		log.Printf("Error occurs when closing session at server. Maybe server is down. Error message: %v %v", status.Code, err)
		return err
	}
	trans.Close()
	return nil
}

func (session *Session) SetStorageGroup(storageGroupId string) error {
	status, err := client.SetStorageGroup(context.Background(), sessionId, storageGroupId)
	log.Printf("setting storage group %v message: %v", storageGroupId, status.Code)
	return err
}

func (session *Session) DeleteStorageGroup(storageGroupId string) error {
	status, err := client.DeleteStorageGroups(context.Background(), sessionId, []string{storageGroupId})
	log.Printf("delete storage group %v message: %v", storageGroupId, status.Code)
	return err
}

func (session *Session) DeleteStorageGroups(storageGroupIds []string) error {
	status, err := client.DeleteStorageGroups(context.Background(), sessionId, storageGroupIds)
	log.Printf("delete storage group %v message: %v", strings.Replace(strings.Trim(fmt.Sprint(storageGroupIds), "[]"), " ", ",", -1), status.Code)
	return err
}

func (session *Session) CreateTimeseries(path string, dataType int32, encoding int32, compressor int32) error {
	request := rpc.TSCreateTimeseriesReq{SessionId: sessionId, Path: path, DataType: dataType, Encoding: encoding, Compressor: compressor}
	status, err := client.CreateTimeseries(context.Background(), &request)
	log.Printf("creating time series %v message: %v", path, status.Code)
	return err
}

func (session *Session) CreateMultiTimeseries(paths []string, dataTypes []int32, encodings []int32, compressors []int32) error {
	request := rpc.TSCreateMultiTimeseriesReq{SessionId: sessionId, Paths: paths, DataTypes: dataTypes, Encodings: encodings, Compressors: compressors}
	status, err := client.CreateMultiTimeseries(context.Background(), &request)
	log.Printf("creating time series %v message: %v", strings.Replace(strings.Trim(fmt.Sprint(paths), "[]"), " ", ",", -1), status.Code)
	return err
}

func (session *Session) DeleteTimeseries(paths []string) error {
	status, err := client.DeleteTimeseries(context.Background(), sessionId, paths)
	log.Printf("delete time series %v message: %v", strings.Replace(strings.Trim(fmt.Sprint(paths), "[]"), " ", ",", -1), status.Code)
	return err
}

func (session *Session) DeleteData(paths []string, startTime int64, endTime int64) error {
	request := rpc.TSDeleteDataReq{SessionId: sessionId, Paths: paths, StartTime: startTime, EndTime: endTime}
	status, err := client.DeleteData(context.Background(), &request)
	log.Printf("delete data from %v message: %v", strings.Replace(strings.Trim(fmt.Sprint(paths), "[]"), " ", ",", -1), status.Code)
	return err
}

func (session *Session) InsertStringRecord(deviceId string, measurements []string, values []string, timestamp int64) error {
	request := rpc.TSInsertStringRecordReq{SessionId: sessionId, DeviceId: deviceId, Measurements: measurements, Values: values, Timestamp: timestamp}
	status, err := client.InsertStringRecord(context.Background(), &request)
	log.Printf("insert one record to device %v message: %v", deviceId, status.Code)
	return err
}

func (session *Session) GetTimeZone() string {
	if session.ZoneId != "" {
		return session.ZoneId
	} else {
		resp, err := client.GetTimeZone(context.Background(), sessionId)
		if err != nil {
			log.Printf("get timezone failed, err: %v", err)
		}
		return resp.TimeZone
	}
}

func (session *Session) SetTimeZone(timeZone string) error {
	request := rpc.TSSetTimeZoneReq{SessionId: sessionId, TimeZone: timeZone}
	status, err := client.SetTimeZone(context.Background(), &request)
	session.ZoneId = timeZone
	log.Printf("set timeZone as %v message: %v", timeZone, status.Code)
	return err
}

func (session *Session) ExecuteStatement(sql string) (*rpc.TSExecuteStatementResp, error) {
	request := rpc.TSExecuteStatementReq{SessionId: sessionId, Statement: sql, StatementId: requestStatementId, FetchSize: &session.FetchSize}
	resp, err := client.ExecuteStatement(context.Background(), &request)
	return resp, err
}

func (session *Session) InsertRecord(deviceId string, measurements []string, dataTypes []int32, values []interface{}, timestamp int64) error {
	/*request := rpc.TSInsertRecordReq{SessionId: sessionId, DeviceId: deviceId, Measurements: measurements, Timestamp: timestamp}
	request.Values = session.genRecordValues(dataTypes, values)
	requests :=rpc.NewTSInsertRecordReq()
	requests.
	status, err:=client.InsertRecord(context.Background(),&request)
	println("insert one record to device" , deviceId, "message:",status.Code)
	println(values)
	return err*/
	return nil
}
func (session *Session) genRecordValues(dataTypes []int32, values []interface{}) []byte {
	return []byte{1}
}
