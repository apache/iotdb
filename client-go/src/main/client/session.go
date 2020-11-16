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

package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/sirupsen/logrus"
	"github.com/yanhongwangg/go-thrift/rpc"
	"github.com/yanhongwangg/incubator-iotdb/client-go/src/main/client/utils"
	"net"
	"os"
	"strings"
	"time"
)

const protocolVersion = rpc.TSProtocolVersion_IOTDB_SERVICE_PROTOCOL_V3

var (
	client             *rpc.TSIServiceClient
	sessionId          int64
	trans              thrift.TTransport
	err                error
	requestStatementId int64
	ts                 string
	sg                 string
	dv                 string
	Log                = logrus.New()
)

func (s *Session) Open(enableRPCCompression bool, connectionTimeoutInMs int) {
	dir, _ := os.Getwd()
	os.Mkdir(dir+"\\logs", os.ModePerm)
	logFile, _ := os.OpenFile(dir+"\\logs\\all.log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	Log.SetOutput(logFile)
	var protocolFactory thrift.TProtocolFactory
	trans, err = thrift.NewTSocketTimeout(net.JoinHostPort(s.Host, s.Port), time.Duration(connectionTimeoutInMs))
	if err != nil {
		Log.WithError(err).Error("connect failed")
	}
	trans = thrift.NewTFramedTransport(trans)
	if !trans.IsOpen() {
		err = trans.Open()
		if err != nil {
			Log.WithError(err).Error("open the conn failed")
		}
	}
	if enableRPCCompression {
		protocolFactory = thrift.NewTCompactProtocolFactory()
	} else {
		protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
	}
	iProtocol := protocolFactory.GetProtocol(trans)
	oProtocol := protocolFactory.GetProtocol(trans)
	client = rpc.NewTSIServiceClient(thrift.NewTStandardClient(iProtocol, oProtocol))
	s.ZoneId = DefaultZoneId
	tSOpenSessionReq := rpc.TSOpenSessionReq{ClientProtocol: protocolVersion, ZoneId: s.ZoneId, Username: &s.User,
		Password: &s.Passwd}
	tSOpenSessionResp, err := client.OpenSession(context.Background(), &tSOpenSessionReq)
	if err != nil {
		Log.WithError(err).Error("open session failed")
	} else {
		Log.WithField("code", tSOpenSessionResp.GetStatus().Code).Info("open session success")
	}
	sessionId = tSOpenSessionResp.GetSessionId()
	requestStatementId, err = client.RequestStatementId(context.Background(), sessionId)
	s.FetchSize = DefaultFetchSize
	if err != nil {
		Log.WithError(err).Error("request StatementId failed")
	}
	if s.ZoneId != "" {
		s.SetTimeZone(s.ZoneId)
	} else {
		s.ZoneId = s.GetTimeZone()
	}
}

func (s *Session) CheckTimeseriesExists(path string) bool {
	dataSet := s.ExecuteQueryStatement("SHOW TIMESERIES " + path)
	result := dataSet.HasNext()
	dataSet.CloseOperationHandle()
	return result
}

func (s *Session) Close() error {
	tSCloseSessionReq := rpc.NewTSCloseSessionReq()
	tSCloseSessionReq.SessionId = sessionId
	status, err := client.CloseSession(context.Background(), tSCloseSessionReq)
	if err != nil {
		Log.WithError(err).Error("Error occurs when closing session at server. Maybe server is down. Error message")
		return err
	} else {
		Log.WithField("code", status.Code).Info("close session success")
	}
	trans.Close()
	return nil
}

/*
 *set one storage group
 *
 *param
 *storageGroupId: string, storage group name (starts from root)
 *
 *return
 *error: correctness of operation
 */
func (s *Session) SetStorageGroup(storageGroupId string) error {
	status, err := client.SetStorageGroup(context.Background(), sessionId, storageGroupId)
	if err != nil {
		Log.WithError(err).Error("setting storage group failed")
	} else {
		Log.WithFields(logrus.Fields{
			"sg":   storageGroupId,
			"code": status.Code,
		}).Info("setting storage group success")
	}
	return err
}

/*
 *delete one storage group
 *
 *param
 *storageGroupId: string, storage group name (starts from root)
 *
 *return
 *error: correctness of operation
 */
func (s *Session) DeleteStorageGroup(storageGroupId string) {
	status, err := client.DeleteStorageGroups(context.Background(), sessionId, []string{storageGroupId})
	if err != nil {
		Log.WithError(err).Error("delete storage group failed")
	} else {
		Log.WithFields(logrus.Fields{
			"sg":   storageGroupId,
			"code": status.Code,
		}).Info("delete storage group success")
	}
}

/*
 *delete multiple storage group
 *
 *param
 *storageGroupIds: []string, paths of the target storage groups
 *
 *return
 *error: correctness of operation
 */
func (s *Session) DeleteStorageGroups(storageGroupIds []string) {
	status, err := client.DeleteStorageGroups(context.Background(), sessionId, storageGroupIds)
	sg = strings.Replace(strings.Trim(fmt.Sprint(storageGroupIds), "[]"), " ", ",", -1)
	if err != nil {
		Log.WithError(err).Error("delete storage groups failed")
	} else {
		Log.WithFields(logrus.Fields{
			"sg":   sg,
			"code": status.Code,
		}).Info("delete storage groups success")
	}
}

/*
 *create single time series
 *
 *params
 *path: string, complete time series path (starts from root)
 *dataType: int32, data type for this time series
 *encoding: int32, data type for this time series
 *compressor: int32, compressing type for this time series
 *
 *return
 *error: correctness of operation
 */
func (s *Session) CreateTimeseries(path string, dataType int32, encoding int32, compressor int32) {
	request := rpc.TSCreateTimeseriesReq{SessionId: sessionId, Path: path, DataType: dataType, Encoding: encoding,
		Compressor: compressor}
	status, err := client.CreateTimeseries(context.Background(), &request)
	if err != nil {
		Log.WithError(err).Error("creating time series failed")
	} else {
		Log.WithFields(logrus.Fields{
			"ts":   path,
			"code": status.Code,
		}).Info("creating time series success")
	}
}

/*
 *create multiple time series
 *
 *params
 *paths: []string, complete time series paths (starts from root)
 *dataTypes: []int32, data types for time series
 *encodings: []int32, encodings for time series
 *compressors: []int32, compressing types for time series
 *
 *return
 *error: correctness of operation
 */
func (s *Session) CreateMultiTimeseries(paths []string, dataTypes []int32, encodings []int32, compressors []int32) {
	request := rpc.TSCreateMultiTimeseriesReq{SessionId: sessionId, Paths: paths, DataTypes: dataTypes,
		Encodings: encodings, Compressors: compressors}
	status, err := client.CreateMultiTimeseries(context.Background(), &request)
	ts = strings.Replace(strings.Trim(fmt.Sprint(paths), "[]"), " ", ",", -1)
	if err != nil {
		Log.WithError(err).Error("creating multi time series failed")
	} else {
		Log.WithFields(logrus.Fields{
			"ts":   ts,
			"code": status.Code,
		}).Info("creating multi time series success")
	}
}

/*
 *delete multiple time series, including data and schema
 *
 *params
 *paths: []string, time series paths, which should be complete (starts from root)
 *
 *return
 *error: correctness of operation
 */
func (s *Session) DeleteTimeseries(paths []string) {
	status, err := client.DeleteTimeseries(context.Background(), sessionId, paths)
	var ts = strings.Replace(strings.Trim(fmt.Sprint(paths), "[]"), " ", ",", -1)
	if err != nil {
		Log.WithError(err).Error("delete time series failed")
	} else {
		Log.WithFields(logrus.Fields{
			"ts":   ts,
			"code": status.Code,
		}).Info("delete time series success")
	}
}

/*
 *delete all startTime <= data <= endTime in multiple time series
 *
 *params
 *paths: []string, time series array that the data in
 *startTime: int64, start time of deletion range
 *endTime: int64, end time of deletion range
 *
 *return
 *error: correctness of operation
 */
func (s *Session) DeleteData(paths []string, startTime int64, endTime int64) {
	request := rpc.TSDeleteDataReq{SessionId: sessionId, Paths: paths, StartTime: startTime, EndTime: endTime}
	status, err := client.DeleteData(context.Background(), &request)
	ts = strings.Replace(strings.Trim(fmt.Sprint(paths), "[]"), " ", ",", -1)
	if err != nil {
		Log.WithError(err).Error("delete data failed")
	} else {
		Log.WithFields(logrus.Fields{
			"ts":   ts,
			"code": status.Code,
		}).Info("delete data success")
	}
}

/*
 *special case for inserting one row of String (TEXT) value
 *
 *params
 *deviceId: string, time series path for device
 *measurements: []string, sensor names
 *values: []string, values to be inserted, for each sensor
 *timestamp: int64, indicate the timestamp of the row of data
 *
 *return
 *error: correctness of operation
 */
func (s *Session) InsertStringRecord(deviceId string, measurements []string, values []string, timestamp int64) {
	request := rpc.TSInsertStringRecordReq{SessionId: sessionId, DeviceId: deviceId, Measurements: measurements,
		Values: values, Timestamp: timestamp}
	status, err := client.InsertStringRecord(context.Background(), &request)
	if err != nil {
		Log.WithError(err).Error("insert one string record failed")
	} else {
		Log.WithFields(logrus.Fields{
			"dv":   deviceId,
			"code": status.Code,
		}).Info("insert one string record success")
	}
}

func (s *Session) TestInsertStringRecord(deviceId string, measurements []string, values []string, timestamp int64) {
	request := rpc.TSInsertStringRecordReq{SessionId: sessionId, DeviceId: deviceId, Measurements: measurements,
		Values: values, Timestamp: timestamp}
	status, err := client.TestInsertStringRecord(context.Background(), &request)
	if err != nil {
		Log.WithError(err).Error("insert one string record failed")
	} else {
		Log.WithFields(logrus.Fields{
			"dv":   deviceId,
			"code": status.Code,
		}).Info("insert one string record success")
	}
}

/*
 *special case for inserting multiple rows of String (TEXT) value
 *
 *params
 *deviceIds: []string, time series paths for device
 *measurements: [][]string, each element of outer list indicates measurements of a device
 *values: [][]interface{}, values to be inserted, for each device
 *timestamps: []int64, timestamps for records
 *
 *return
 *error: correctness of operation
 */
func (s *Session) InsertStringRecords(deviceIds []string, measurements [][]string, values [][]string,
	timestamps []int64) {
	request := rpc.TSInsertStringRecordsReq{SessionId: sessionId, DeviceIds: deviceIds, MeasurementsList: measurements,
		ValuesList: values, Timestamps: timestamps}
	dv = strings.Replace(strings.Trim(fmt.Sprint(deviceIds), "[]"), " ", ",", -1)
	status, err := client.InsertStringRecords(context.Background(), &request)
	if err != nil {
		Log.WithError(err).Error("insert multi string records failed")
	} else {
		Log.WithFields(logrus.Fields{
			"dv":   dv,
			"code": status.Code,
		}).Info("insert multi string records success")
	}
}

func (s *Session) TestInsertStringRecords(deviceIds []string, measurements [][]string, values [][]string,
	timestamps []int64) {
	request := rpc.TSInsertStringRecordsReq{SessionId: sessionId, DeviceIds: deviceIds, MeasurementsList: measurements,
		ValuesList: values, Timestamps: timestamps}
	dv = strings.Replace(strings.Trim(fmt.Sprint(deviceIds), "[]"), " ", ",", -1)
	status, err := client.TestInsertStringRecords(context.Background(), &request)
	if err != nil {
		Log.WithError(err).Error("insert multi string records failed")
	} else {
		Log.WithFields(logrus.Fields{
			"dv":   dv,
			"code": status.Code,
		}).Info("insert multi string records success")
	}
}

/*
 *insert one row of record into database, if you want improve your performance, please use insertTablet method
 *
 *params
 *deviceId: string, time series path for device
 *measurements: []string, sensor names
 *dataTypes: []int32, list of dataType, indicate the data type for each sensor
 *values: []interface{}, values to be inserted, for each sensor
 *timestamp: int64, indicate the timestamp of the row of data
 *
 *return
 *error: correctness of operation
 */
func (s *Session) InsertRecord(deviceId string, measurements []string, dataTypes []int32, values []interface{},
	timestamp int64) {
	request := genInsertRecordReq(deviceId, measurements, dataTypes, values, timestamp)
	status, err := client.InsertRecord(context.Background(), request)
	if err != nil {
		Log.WithError(err).Error("insert one record failed")
	} else {
		Log.WithFields(logrus.Fields{
			"dv":   deviceId,
			"code": status.Code,
		}).Info("insert one record success")
	}
}

func (s *Session) TestInsertRecord(deviceId string, measurements []string, dataTypes []int32, values []interface{},
	timestamp int64) {
	request := genInsertRecordReq(deviceId, measurements, dataTypes, values, timestamp)
	status, err := client.TestInsertRecord(context.Background(), request)
	if err != nil {
		Log.WithError(err).Error("insert one record failed")
	} else {
		Log.WithFields(logrus.Fields{
			"dv":   deviceId,
			"code": status.Code,
		}).Info("insert one record success")
	}
}

func genInsertRecordReq(deviceId string, measurements []string, dataTypes []int32, values []interface{},
	timestamp int64) *rpc.TSInsertRecordReq {
	request := rpc.TSInsertRecordReq{SessionId: sessionId, DeviceId: deviceId, Measurements: measurements,
		Timestamp: timestamp}
	request.Values = valuesToBytes(dataTypes, values)
	return &request
}

/*
 *insert multiple rows of data, records are independent to each other, in other words, there's no relationship
 *between those records
 *
 *params
 *deviceIds: []string, time series paths for device
 *measurements: [][]string, each element of outer list indicates measurements of a device
 *dataTypes: [][]int32, each element of outer list indicates sensor data types of a device
 *values: [][]interface{}, values to be inserted, for each device
 *timestamps: []int64, timestamps for records
 *
 *return
 *error: correctness of operation
 */
func (s *Session) InsertRecords(deviceIds []string, measurements [][]string, dataTypes [][]int32, values [][]interface{},
	timestamps []int64) {
	dv = strings.Replace(strings.Trim(fmt.Sprint(deviceIds), "[]"), " ", ",", -1)
	request := genInsertRecordsReq(deviceIds, measurements, dataTypes, values, timestamps)
	status, err := client.InsertRecords(context.Background(), request)
	if err != nil {
		Log.WithError(err).Error("insert multiple records failed")
	} else {
		Log.WithFields(logrus.Fields{
			"dv":   dv,
			"code": status.Code,
		}).Info("insert multiple records success")
	}
}

func (s *Session) TestInsertRecords(deviceIds []string, measurements [][]string, dataTypes [][]int32, values [][]interface{},
	timestamps []int64) {
	dv = strings.Replace(strings.Trim(fmt.Sprint(deviceIds), "[]"), " ", ",", -1)
	request := genInsertRecordsReq(deviceIds, measurements, dataTypes, values, timestamps)
	status, err := client.TestInsertRecords(context.Background(), request)
	if err != nil {
		Log.WithError(err).Error("insert multiple records failed")
	} else {
		Log.WithFields(logrus.Fields{
			"dv":   dv,
			"code": status.Code,
		}).Info("insert multiple records success")
	}
}

func genInsertRecordsReq(deviceIds []string, measurements [][]string, dataTypes [][]int32, values [][]interface{},
	timestamps []int64) *rpc.TSInsertRecordsReq {
	length := len(deviceIds)
	if length != len(timestamps) || length != len(measurements) || length != len(values) {
		Log.Error("deviceIds, times, measurementsList and valuesList's size should be equal")
		return nil
	}
	request := rpc.TSInsertRecordsReq{SessionId: sessionId, DeviceIds: deviceIds, MeasurementsList: measurements,
		Timestamps: timestamps}
	v := make([][]byte, length)
	for i := 0; i < len(measurements); i++ {
		v[i] = valuesToBytes(dataTypes[i], values[i])
	}
	request.ValuesList = v
	return &request
}

/*
 *insert one tablet, in a tablet, for each timestamp, the number of measurements is same
 *
 *params
 *tablet: utils.Tablet, a tablet specified above
 *
 *return
 *error: correctness of operation
 */
func (s *Session) InsertTablet(tablet utils.Tablet) {
	tablet.SortTablet()
	request := genInsertTabletReq(tablet)
	status, err := client.InsertTablet(context.Background(), request)
	if err != nil {
		Log.WithError(err).Error("insert tablet failed")
	} else {
		Log.WithField("code", status.Code).Info("insert tablet success")
	}
}

func (s *Session) TestInsertTablet(tablet utils.Tablet) {
	tablet.SortTablet()
	request := genInsertTabletReq(tablet)
	status, err := client.TestInsertTablet(context.Background(), request)
	if err != nil {
		Log.WithError(err).Error("insert tablet failed")
	} else {
		Log.WithField("code", status.Code).Info("insert tablet success")
	}
}

func genInsertTabletReq(tablet utils.Tablet) *rpc.TSInsertTabletReq {
	request := rpc.TSInsertTabletReq{SessionId: sessionId, DeviceId: tablet.GetDeviceId(), Types: tablet.GetTypes(),
		Measurements: tablet.Measurements, Values: tablet.GetBinaryValues(), Timestamps: tablet.GetBinaryTimestamps(),
		Size: tablet.GetRowNumber()}
	return &request
}

/*
 *insert multiple tablets, tablets are independent to each other
 *
 *params
 *tablets: []utils.Tablet, list of tablets
 *
 *return
 *error: correctness of operation
 */
func (s *Session) InsertTablets(tablets []utils.Tablet) {
	for index := range tablets {
		tablets[index].SortTablet()
	}
	request := genInsertTabletsReq(tablets)
	status, err := client.InsertTablets(context.Background(), request)
	if err != nil {
		Log.WithError(err).Error("insert tablets failed")
	} else {
		Log.WithField("code", status.Code).Info("insert tablets success")
	}
}

func (s *Session) TestInsertTablets(tablets []utils.Tablet) {
	for index := range tablets {
		tablets[index].SortTablet()
	}
	request := genInsertTabletsReq(tablets)
	status, err := client.TestInsertTablets(context.Background(), request)
	if err != nil {
		Log.WithError(err).Error("insert tablets failed")
	} else {
		Log.WithField("code", status.Code).Info("insert tablets success")
	}
}

func genInsertTabletsReq(tablets []utils.Tablet) *rpc.TSInsertTabletsReq {
	var (
		length           = len(tablets)
		deviceIds        = make([]string, length)
		measurementsList = make([][]string, length)
		valuesList       = make([][]byte, length)
		timestampsList   = make([][]byte, length)
		typesList        = make([][]int32, length)
		sizeList         = make([]int32, length)
	)
	for index, tablet := range tablets {
		deviceIds[index] = tablet.GetDeviceId()
		measurementsList[index] = tablet.GetMeasurements()
		valuesList[index] = tablet.GetBinaryValues()
		timestampsList[index] = tablet.GetBinaryTimestamps()
		typesList[index] = tablet.GetTypes()
		sizeList[index] = tablet.GetRowNumber()
	}
	request := rpc.TSInsertTabletsReq{SessionId: sessionId, DeviceIds: deviceIds, TypesList: typesList,
		MeasurementsList: measurementsList, ValuesList: valuesList, TimestampsList: timestampsList,
		SizeList: sizeList}
	return &request
}

func valuesToBytes(dataTypes []int32, values []interface{}) []byte {
	buf := bytes.NewBuffer([]byte{})
	for i := 0; i < len(dataTypes); i++ {
		dataType := int16(dataTypes[i])
		binary.Write(buf, binary.BigEndian, &dataType)
		switch dataTypes[i] {
		case 0:
			binary.Write(buf, binary.BigEndian, values[i].(bool))
			break
		case 1:
			tmp := values[i].(int32)
			binary.Write(buf, binary.BigEndian, &tmp)
			break
		case 2:
			tmp := values[i].(int64)
			binary.Write(buf, binary.BigEndian, &tmp)
			break
		case 3:
			tmp := values[i].(float32)
			binary.Write(buf, binary.BigEndian, &tmp)
			break
		case 4:
			tmp := values[i].(float64)
			binary.Write(buf, binary.BigEndian, &tmp)
			break
		case 5:
			tmp := (int32)(len(values[i].(string)))
			binary.Write(buf, binary.BigEndian, &tmp)
			buf.WriteString(values[i].(string))
			break
		case 6:
			tmp := values[i].(int)
			binary.Write(buf, binary.BigEndian, &tmp)
			break
		}
	}
	return buf.Bytes()
}

func (s *Session) ExecuteStatement(sql string) *utils.SessionDataSet {
	request := rpc.TSExecuteStatementReq{SessionId: sessionId, Statement: sql, StatementId: requestStatementId,
		FetchSize: &s.FetchSize}
	resp, _ := client.ExecuteStatement(context.Background(), &request)
	dataSet := s.genDataSet(sql, resp)
	sessionDataSet := utils.NewSessionDataSet(dataSet)
	return sessionDataSet
}

func (s *Session) genDataSet(sql string, resp *rpc.TSExecuteStatementResp) *utils.SessionDataSet {
	dataSet := utils.SessionDataSet{
		Sql:             sql,
		ColumnNameList:  resp.GetColumns(),
		ColumnTypeList:  resp.GetDataTypeList(),
		ColumnNameIndex: resp.GetColumnNameIndexMap(),
		QueryId:         resp.GetQueryId(),
		SessionId:       sessionId,
		IgnoreTimeStamp: resp.GetIgnoreTimeStamp(),
		Client:          client,
		QueryDataSet:    resp.GetQueryDataSet(),
	}
	return &dataSet
}

func (s *Session) ExecuteQueryStatement(sql string) *utils.SessionDataSet {
	request := rpc.TSExecuteStatementReq{SessionId: sessionId, Statement: sql, StatementId: requestStatementId,
		FetchSize: &s.FetchSize}
	resp, _ := client.ExecuteQueryStatement(context.Background(), &request)
	dataSet := s.genDataSet(sql, resp)
	sessionDataSet := utils.NewSessionDataSet(dataSet)
	return sessionDataSet
}

func (s *Session) ExecuteUpdateStatement(sql string) *utils.SessionDataSet {
	request := rpc.TSExecuteStatementReq{SessionId: sessionId, Statement: sql, StatementId: requestStatementId,
		FetchSize: &s.FetchSize}
	resp, _ := client.ExecuteUpdateStatement(context.Background(), &request)
	dataSet := s.genDataSet(sql, resp)
	sessionDataSet := utils.NewSessionDataSet(dataSet)
	return sessionDataSet
}

func (s *Session) ExecuteBatchStatement(inserts []string) {
	request := rpc.TSExecuteBatchStatementReq{
		SessionId:  sessionId,
		Statements: inserts,
	}
	status, err := client.ExecuteBatchStatement(context.Background(), &request)
	if err != nil {
		Log.WithError(err).Error("insert batch data failed")
	} else {
		Log.WithField("code", status.Code).Info("insert batch data success")
	}
}

func (s *Session) ExecuteRawDataQuery(paths []string, startTime int64, endTime int64) *utils.SessionDataSet {
	request := rpc.TSRawDataQueryReq{
		SessionId:   sessionId,
		Paths:       paths,
		FetchSize:   &s.FetchSize,
		StartTime:   startTime,
		EndTime:     endTime,
		StatementId: requestStatementId,
	}
	resp, _ := client.ExecuteRawDataQuery(context.Background(), &request)
	dataSet := s.genDataSet("", resp)
	sessionDataSet := utils.NewSessionDataSet(dataSet)
	return sessionDataSet
}

func (s *Session) GetTimeZone() string {
	if s.ZoneId != "" {
		return s.ZoneId
	} else {
		resp, err := client.GetTimeZone(context.Background(), sessionId)
		if err != nil {
			Log.Error("get timezone failed ", err)
		} else {
			Log.Info("get timezone success")
		}
		return resp.TimeZone
	}
}

func (s *Session) SetTimeZone(timeZone string) error {
	request := rpc.TSSetTimeZoneReq{SessionId: sessionId, TimeZone: timeZone}
	status, err := client.SetTimeZone(context.Background(), &request)
	s.ZoneId = timeZone
	if err != nil {
		Log.Error("set timeZone as ", timeZone, " failed ", err)
	} else {
		Log.Info("set timeZone as ", timeZone, " success ", status.Code)
	}
	return err
}
