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
	log "github.com/sirupsen/logrus"
	"github.com/yanhongwangg/go-thrift/rpc"
	"github.com/yanhongwangg/incubator-iotdb/client-go/src/main/client/utils"
	"net"
	"os"
	"strings"
	"time"
)

const protocolVersion = rpc.TSProtocolVersion_IOTDB_SERVICE_PROTOCOL_V3

func (s *Session) Open(enableRPCCompression bool, connectionTimeoutInMs int) {
	dir, _ := os.Getwd()
	os.Mkdir(dir+"\\logs", os.ModePerm)
	logFile, _ := os.OpenFile(dir+"\\logs\\all.log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	log.SetOutput(logFile)
	log.SetLevel(log.InfoLevel)
	var protocolFactory thrift.TProtocolFactory
	s.trans, s.err = thrift.NewTSocketTimeout(net.JoinHostPort(s.Host, s.Port), time.Duration(connectionTimeoutInMs))
	if s.err != nil {
		log.WithError(s.err).Error("connect failed")
	}
	s.trans = thrift.NewTFramedTransport(s.trans)
	if !s.trans.IsOpen() {
		s.err = s.trans.Open()
		if s.err != nil {
			log.WithError(s.err).Error("open the conn failed")
		}
	}
	if enableRPCCompression {
		protocolFactory = thrift.NewTCompactProtocolFactory()
	} else {
		protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
	}
	iProtocol := protocolFactory.GetProtocol(s.trans)
	oProtocol := protocolFactory.GetProtocol(s.trans)
	s.client = rpc.NewTSIServiceClient(thrift.NewTStandardClient(iProtocol, oProtocol))
	s.ZoneId = DefaultZoneId
	tSOpenSessionReq := rpc.TSOpenSessionReq{ClientProtocol: protocolVersion, ZoneId: s.ZoneId, Username: &s.User,
		Password: &s.Passwd}
	tSOpenSessionResp, err := s.client.OpenSession(context.Background(), &tSOpenSessionReq)
	if err != nil {
		log.WithError(err).Error("open session failed")
	} else {
		log.WithField("code", tSOpenSessionResp.GetStatus().Code).Debug("open session success")
	}
	s.sessionId = tSOpenSessionResp.GetSessionId()
	s.requestStatementId, err = s.client.RequestStatementId(context.Background(), s.sessionId)
	s.FetchSize = DefaultFetchSize
	if err != nil {
		log.WithError(err).Error("request StatementId failed")
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

func (s *Session) Close() {
	tSCloseSessionReq := rpc.NewTSCloseSessionReq()
	tSCloseSessionReq.SessionId = s.sessionId
	status, err := s.client.CloseSession(context.Background(), tSCloseSessionReq)
	if err != nil {
		log.WithError(err).Error("Error occurs when closing session at server. Maybe server is down.")
	} else {
		log.WithField("code", status.Code).Debug("close session success")
	}
	s.trans.Close()
}

/*
 *set one storage group
 *
 *param
 *storageGroupId: string, storage group name (starts from root)
 *
 */
func (s *Session) SetStorageGroup(storageGroupId string) {
	status, err := s.client.SetStorageGroup(context.Background(), s.sessionId, storageGroupId)
	if err != nil {
		log.WithError(err).Error("setting storage group failed")
	} else {
		log.WithFields(log.Fields{
			"sg":   storageGroupId,
			"code": status.Code,
		}).Debug("setting storage group success")
	}
}

/*
 *delete one storage group
 *
 *param
 *storageGroupId: string, storage group name (starts from root)
 *
 */
func (s *Session) DeleteStorageGroup(storageGroupId string) {
	status, err := s.client.DeleteStorageGroups(context.Background(), s.sessionId, []string{storageGroupId})
	if err != nil {
		log.WithError(err).Error("delete storage group failed")
	} else {
		log.WithFields(log.Fields{
			"sg":   storageGroupId,
			"code": status.Code,
		}).Debug("delete storage group success")
	}
}

/*
 *delete multiple storage group
 *
 *param
 *storageGroupIds: []string, paths of the target storage groups
 *
 */
func (s *Session) DeleteStorageGroups(storageGroupIds []string) {
	status, err := s.client.DeleteStorageGroups(context.Background(), s.sessionId, storageGroupIds)
	s.sg = strings.Replace(strings.Trim(fmt.Sprint(storageGroupIds), "[]"), " ", ",", -1)
	if err != nil {
		log.WithError(err).Error("delete storage groups failed")
	} else {
		log.WithFields(log.Fields{
			"sg":   s.sg,
			"code": status.Code,
		}).Debug("delete storage groups success")
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
 */
func (s *Session) CreateTimeseries(path string, dataType int32, encoding int32, compressor int32) {
	request := rpc.TSCreateTimeseriesReq{SessionId: s.sessionId, Path: path, DataType: dataType, Encoding: encoding,
		Compressor: compressor}
	status, err := s.client.CreateTimeseries(context.Background(), &request)
	if err != nil {
		log.WithError(err).Error("creating time series failed")
	} else {
		log.WithFields(log.Fields{
			"ts":   path,
			"code": status.Code,
		}).Debug("creating time series success")
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
 */
func (s *Session) CreateMultiTimeseries(paths []string, dataTypes []int32, encodings []int32, compressors []int32) {
	request := rpc.TSCreateMultiTimeseriesReq{SessionId: s.sessionId, Paths: paths, DataTypes: dataTypes,
		Encodings: encodings, Compressors: compressors}
	status, err := s.client.CreateMultiTimeseries(context.Background(), &request)
	s.ts = strings.Replace(strings.Trim(fmt.Sprint(paths), "[]"), " ", ",", -1)
	if err != nil {
		log.WithError(err).Error("creating multi time series failed")
	} else {
		log.WithFields(log.Fields{
			"ts":   s.ts,
			"code": status.Code,
		}).Debug("creating multi time series success")
	}
}

/*
 *delete multiple time series, including data and schema
 *
 *params
 *paths: []string, time series paths, which should be complete (starts from root)
 *
 */
func (s *Session) DeleteTimeseries(paths []string) {
	status, err := s.client.DeleteTimeseries(context.Background(), s.sessionId, paths)
	var ts = strings.Replace(strings.Trim(fmt.Sprint(paths), "[]"), " ", ",", -1)
	if err != nil {
		log.WithError(err).Error("delete time series failed")
	} else {
		log.WithFields(log.Fields{
			"ts":   ts,
			"code": status.Code,
		}).Debug("delete time series success")
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
 */
func (s *Session) DeleteData(paths []string, startTime int64, endTime int64) {
	request := rpc.TSDeleteDataReq{SessionId: s.sessionId, Paths: paths, StartTime: startTime, EndTime: endTime}
	status, err := s.client.DeleteData(context.Background(), &request)
	s.ts = strings.Replace(strings.Trim(fmt.Sprint(paths), "[]"), " ", ",", -1)
	if err != nil {
		log.WithError(err).Error("delete data failed")
	} else {
		log.WithFields(log.Fields{
			"ts":   s.ts,
			"code": status.Code,
		}).Debug("delete data success")
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
 */
func (s *Session) InsertStringRecord(deviceId string, measurements []string, values []string, timestamp int64) {
	request := rpc.TSInsertStringRecordReq{SessionId: s.sessionId, DeviceId: deviceId, Measurements: measurements,
		Values: values, Timestamp: timestamp}
	status, err := s.client.InsertStringRecord(context.Background(), &request)
	if err != nil {
		log.WithError(err).Error("insert one string record failed")
	} else {
		log.WithFields(log.Fields{
			"dv":   deviceId,
			"code": status.Code,
		}).Debug("insert one string record success")
	}
}

func (s *Session) TestInsertStringRecord(deviceId string, measurements []string, values []string, timestamp int64) {
	request := rpc.TSInsertStringRecordReq{SessionId: s.sessionId, DeviceId: deviceId, Measurements: measurements,
		Values: values, Timestamp: timestamp}
	status, err := s.client.TestInsertStringRecord(context.Background(), &request)
	if err != nil {
		log.WithError(err).Error("insert one string record failed")
	} else {
		log.WithFields(log.Fields{
			"dv":   deviceId,
			"code": status.Code,
		}).Debug("insert one string record success")
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
 */
func (s *Session) InsertStringRecords(deviceIds []string, measurements [][]string, values [][]string,
	timestamps []int64) {
	request := rpc.TSInsertStringRecordsReq{SessionId: s.sessionId, DeviceIds: deviceIds, MeasurementsList: measurements,
		ValuesList: values, Timestamps: timestamps}
	s.dv = strings.Replace(strings.Trim(fmt.Sprint(deviceIds), "[]"), " ", ",", -1)
	status, err := s.client.InsertStringRecords(context.Background(), &request)
	if err != nil {
		log.WithError(err).Error("insert multi string records failed")
	} else {
		log.WithFields(log.Fields{
			"dv":   s.dv,
			"code": status.Code,
		}).Debug("insert multi string records success")
	}
}

func (s *Session) TestInsertStringRecords(deviceIds []string, measurements [][]string, values [][]string,
	timestamps []int64) {
	request := rpc.TSInsertStringRecordsReq{SessionId: s.sessionId, DeviceIds: deviceIds, MeasurementsList: measurements,
		ValuesList: values, Timestamps: timestamps}
	s.dv = strings.Replace(strings.Trim(fmt.Sprint(deviceIds), "[]"), " ", ",", -1)
	status, err := s.client.TestInsertStringRecords(context.Background(), &request)
	if err != nil {
		log.WithError(err).Error("insert multi string records failed")
	} else {
		log.WithFields(log.Fields{
			"dv":   s.dv,
			"code": status.Code,
		}).Debug("insert multi string records success")
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
 */
func (s *Session) InsertRecord(deviceId string, measurements []string, dataTypes []int32, values []interface{},
	timestamp int64) {
	request := s.genInsertRecordReq(deviceId, measurements, dataTypes, values, timestamp)
	status, err := s.client.InsertRecord(context.Background(), request)
	if err != nil {
		log.WithError(err).Error("insert one record failed")
	} else {
		log.WithFields(log.Fields{
			"dv":   deviceId,
			"code": status.Code,
		}).Debug("insert one record success")
	}
}

func (s *Session) TestInsertRecord(deviceId string, measurements []string, dataTypes []int32, values []interface{},
	timestamp int64) {
	request := s.genInsertRecordReq(deviceId, measurements, dataTypes, values, timestamp)
	status, err := s.client.TestInsertRecord(context.Background(), request)
	if err != nil {
		log.WithError(err).Error("insert one record failed")
	} else {
		log.WithFields(log.Fields{
			"dv":   deviceId,
			"code": status.Code,
		}).Debug("insert one record success")
	}
}

func (s *Session) genInsertRecordReq(deviceId string, measurements []string, dataTypes []int32, values []interface{},
	timestamp int64) *rpc.TSInsertRecordReq {
	request := rpc.TSInsertRecordReq{SessionId: s.sessionId, DeviceId: deviceId, Measurements: measurements,
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
 */
func (s *Session) InsertRecords(deviceIds []string, measurements [][]string, dataTypes [][]int32, values [][]interface{},
	timestamps []int64) {
	s.dv = strings.Replace(strings.Trim(fmt.Sprint(deviceIds), "[]"), " ", ",", -1)
	request := s.genInsertRecordsReq(deviceIds, measurements, dataTypes, values, timestamps)
	status, err := s.client.InsertRecords(context.Background(), request)
	if err != nil {
		log.WithError(err).Error("insert multiple records failed")
	} else {
		log.WithFields(log.Fields{
			"dv":   s.dv,
			"code": status.Code,
		}).Debug("insert multiple records success")
	}
}

func (s *Session) TestInsertRecords(deviceIds []string, measurements [][]string, dataTypes [][]int32, values [][]interface{},
	timestamps []int64) {
	s.dv = strings.Replace(strings.Trim(fmt.Sprint(deviceIds), "[]"), " ", ",", -1)
	request := s.genInsertRecordsReq(deviceIds, measurements, dataTypes, values, timestamps)
	status, err := s.client.TestInsertRecords(context.Background(), request)
	if err != nil {
		log.WithError(err).Error("insert multiple records failed")
	} else {
		log.WithFields(log.Fields{
			"dv":   s.dv,
			"code": status.Code,
		}).Debug("insert multiple records success")
	}
}

func (s *Session) genInsertRecordsReq(deviceIds []string, measurements [][]string, dataTypes [][]int32, values [][]interface{},
	timestamps []int64) *rpc.TSInsertRecordsReq {
	length := len(deviceIds)
	if length != len(timestamps) || length != len(measurements) || length != len(values) {
		log.Error("deviceIds, times, measurementsList and valuesList's size should be equal")
		return nil
	}
	request := rpc.TSInsertRecordsReq{SessionId: s.sessionId, DeviceIds: deviceIds, MeasurementsList: measurements,
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
 */
func (s *Session) InsertTablet(tablet utils.Tablet) {
	tablet.SortTablet()
	request := s.genInsertTabletReq(tablet)
	status, err := s.client.InsertTablet(context.Background(), request)
	if err != nil {
		log.WithError(err).Error("insert tablet failed")
	} else {
		log.WithField("code", status.Code).Debug("insert tablet success")
	}
}

func (s *Session) TestInsertTablet(tablet utils.Tablet) {
	tablet.SortTablet()
	request := s.genInsertTabletReq(tablet)
	status, err := s.client.TestInsertTablet(context.Background(), request)
	if err != nil {
		log.WithError(err).Error("insert tablet failed")
	} else {
		log.WithField("code", status.Code).Debug("insert tablet success")
	}
}

func (s *Session) genInsertTabletReq(tablet utils.Tablet) *rpc.TSInsertTabletReq {
	request := rpc.TSInsertTabletReq{SessionId: s.sessionId, DeviceId: tablet.GetDeviceId(), Types: tablet.GetTypes(),
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
 */
func (s *Session) InsertTablets(tablets []utils.Tablet) {
	for index := range tablets {
		tablets[index].SortTablet()
	}
	request := s.genInsertTabletsReq(tablets)
	status, err := s.client.InsertTablets(context.Background(), request)
	if err != nil {
		log.WithError(err).Error("insert tablets failed")
	} else {
		log.WithField("code", status.Code).Debug("insert tablets success")
	}
}

func (s *Session) TestInsertTablets(tablets []utils.Tablet) {
	for index := range tablets {
		tablets[index].SortTablet()
	}
	request := s.genInsertTabletsReq(tablets)
	status, err := s.client.TestInsertTablets(context.Background(), request)
	if err != nil {
		log.WithError(err).Error("insert tablets failed")
	} else {
		log.WithField("code", status.Code).Debug("insert tablets success")
	}
}

func (s *Session) genInsertTabletsReq(tablets []utils.Tablet) *rpc.TSInsertTabletsReq {
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
	request := rpc.TSInsertTabletsReq{SessionId: s.sessionId, DeviceIds: deviceIds, TypesList: typesList,
		MeasurementsList: measurementsList, ValuesList: valuesList, TimestampsList: timestampsList,
		SizeList: sizeList}
	return &request
}

func valuesToBytes(dataTypes []int32, values []interface{}) []byte {
	buf := bytes.NewBuffer([]byte{})
	for i := 0; i < len(dataTypes); i++ {
		dataType := int16(dataTypes[i])
		binary.Write(buf, binary.BigEndian, dataType)
		switch dataTypes[i] {
		case 0, 1, 2, 3, 4:
			binary.Write(buf, binary.BigEndian, values[i])
			break
		case 5:
			tmp := (int32)(len(values[i].(string)))
			binary.Write(buf, binary.BigEndian, tmp)
			buf.WriteString(values[i].(string))
			break
		}
	}
	return buf.Bytes()
}

func (s *Session) ExecuteStatement(sql string) *utils.SessionDataSet {
	request := rpc.TSExecuteStatementReq{SessionId: s.sessionId, Statement: sql, StatementId: s.requestStatementId,
		FetchSize: &s.FetchSize}
	resp, _ := s.client.ExecuteStatement(context.Background(), &request)
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
		SessionId:       s.sessionId,
		IgnoreTimeStamp: resp.GetIgnoreTimeStamp(),
		Client:          s.client,
		QueryDataSet:    resp.GetQueryDataSet(),
	}
	return &dataSet
}

func (s *Session) ExecuteQueryStatement(sql string) *utils.SessionDataSet {
	request := rpc.TSExecuteStatementReq{SessionId: s.sessionId, Statement: sql, StatementId: s.requestStatementId,
		FetchSize: &s.FetchSize}
	resp, _ := s.client.ExecuteQueryStatement(context.Background(), &request)
	dataSet := s.genDataSet(sql, resp)
	sessionDataSet := utils.NewSessionDataSet(dataSet)
	return sessionDataSet
}

func (s *Session) ExecuteUpdateStatement(sql string) *utils.SessionDataSet {
	request := rpc.TSExecuteStatementReq{SessionId: s.sessionId, Statement: sql, StatementId: s.requestStatementId,
		FetchSize: &s.FetchSize}
	resp, _ := s.client.ExecuteUpdateStatement(context.Background(), &request)
	dataSet := s.genDataSet(sql, resp)
	sessionDataSet := utils.NewSessionDataSet(dataSet)
	return sessionDataSet
}

func (s *Session) ExecuteBatchStatement(inserts []string) {
	request := rpc.TSExecuteBatchStatementReq{
		SessionId:  s.sessionId,
		Statements: inserts,
	}
	status, err := s.client.ExecuteBatchStatement(context.Background(), &request)
	if err != nil {
		log.WithError(err).Error("insert batch data failed")
	} else {
		log.WithField("code", status.Code).Debug("insert batch data success")
	}
}

func (s *Session) ExecuteRawDataQuery(paths []string, startTime int64, endTime int64) *utils.SessionDataSet {
	request := rpc.TSRawDataQueryReq{
		SessionId:   s.sessionId,
		Paths:       paths,
		FetchSize:   &s.FetchSize,
		StartTime:   startTime,
		EndTime:     endTime,
		StatementId: s.requestStatementId,
	}
	resp, _ := s.client.ExecuteRawDataQuery(context.Background(), &request)
	dataSet := s.genDataSet("", resp)
	sessionDataSet := utils.NewSessionDataSet(dataSet)
	return sessionDataSet
}

func (s *Session) GetTimeZone() string {
	if s.ZoneId != "" {
		return s.ZoneId
	} else {
		resp, err := s.client.GetTimeZone(context.Background(), s.sessionId)
		if err != nil {
			log.Error("get timezone failed ", err)
		} else {
			log.Debug("get timezone success")
		}
		return resp.TimeZone
	}
}

func (s *Session) SetTimeZone(timeZone string) {
	request := rpc.TSSetTimeZoneReq{SessionId: s.sessionId, TimeZone: timeZone}
	status, err := s.client.SetTimeZone(context.Background(), &request)
	s.ZoneId = timeZone
	if err != nil {
		log.Error("set timeZone as ", timeZone, " failed ", err)
	} else {
		log.Debug("set timeZone as ", timeZone, " success ", status.Code)
	}
}
