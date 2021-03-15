package utils

import (
	"bytes"
	"client-go/gen-go/rpc"
	"context"
	"encoding/binary"
	"fmt"
)

var timestamp_str = "Time"
var start_index int32 = 2
var flag = 0x80

type IoTDBRpcDataSet struct {
	sessionId                  int64
	ignoreTimestamp            bool
	sql                        string
	queryId                    int64
	client                     *rpc.TSIServiceClient
	fetchSize                  int32
	columnSize                 int
	defaultTimeOut             int64
	columnNameList             []string
	columnTypeList             []int32
	columnOrdinalDict          map[string]int32
	columnTypeDeduplicatedList []int32
	timeBytes                  []byte
	currentBitMap              []byte
	value                      [][]byte
	queryDataSet               *rpc.TSQueryDataSet
	isClosed                   bool
	emptyResultSet             bool
	hasCachedRecord            bool
	rowsIndex                  int
}

func NewIoTDBRpcDataSet(sql string, columnNameList []string, columnTypeList []int32, columnName2Index map[string]int32, ignoreTimestamp bool, queryId int64, client *rpc.TSIServiceClient, sessionId int64, queryDataSet *rpc.TSQueryDataSet, fetchSize int32) *IoTDBRpcDataSet {
	r := &IoTDBRpcDataSet{}
	r.sql = sql
	r.sessionId = sessionId
	r.ignoreTimestamp = ignoreTimestamp
	r.queryId = queryId
	r.client = client
	r.fetchSize = fetchSize
	r.columnSize = len(columnNameList)
	r.defaultTimeOut = 1000
	r.columnNameList = make([]string, 0)
	r.columnTypeList = make([]int32, 0)
	r.columnOrdinalDict = make(map[string]int32)

	if !ignoreTimestamp {
		r.columnNameList = append(r.columnNameList, timestamp_str)
		r.columnTypeList = append(r.columnTypeList, TSDataType.INT64)
		r.columnOrdinalDict[timestamp_str] = 1
	}

	if len(columnName2Index) > 0 {
		r.columnTypeDeduplicatedList = make([]int32, len(columnName2Index))
		for k, name := range columnNameList {
			r.columnNameList = append(r.columnNameList, name)
			r.columnTypeList = append(r.columnTypeList, columnTypeList[k])
			if _, ok := r.columnOrdinalDict[name]; !ok {
				index := columnName2Index[name]
				r.columnOrdinalDict[name] = index + start_index
				r.columnTypeDeduplicatedList[index] = columnTypeList[k]
			}
		}
	} else {
		index := start_index
		for k, name := range columnNameList {
			r.columnNameList = append(r.columnNameList, name)
			r.columnTypeList = append(r.columnTypeList, columnTypeList[k])
			if _, ok := r.columnOrdinalDict[name]; !ok {
				r.columnOrdinalDict[name] = index
				index += 1
				r.columnTypeDeduplicatedList = append(r.columnTypeDeduplicatedList, columnTypeList[k])
			}
		}
	}

	r.currentBitMap = make([]byte, len(r.columnTypeDeduplicatedList))
	r.queryDataSet = queryDataSet
	r.isClosed = false
	r.emptyResultSet = false
	r.hasCachedRecord = false
	r.rowsIndex = 0
	return r
}

func (r_ *IoTDBRpcDataSet) Close() {
	if r_.isClosed {
		return
	}
	if r_.client != nil {
		status, _ := r_.client.CloseOperation(context.Background(), &rpc.TSCloseOperationReq{SessionId: r_.sessionId, QueryId: &r_.queryId})
		fmt.Printf("Close IoTDBRpcDataSet Session{%v}, message{%v}\n", r_.sessionId, status.GetMessage())
		r_.isClosed = true
		r_.client = nil
	}
}

func (r_ *IoTDBRpcDataSet) Next() bool {
	if r_.HasCachedResult() {
		r_.ConstructOneRow()
		return true
	}
	if r_.emptyResultSet {
		return false
	}
	if r_.fetchResults() {
		r_.ConstructOneRow()
		return true
	}
	return false
}

func (r_ *IoTDBRpcDataSet) HasCachedResult() bool {
	return r_.queryDataSet != nil && len(r_.queryDataSet.Time) != 0
}

func (r_ *IoTDBRpcDataSet) IsNull(index int32, rowNum int) bool {
	bitMap := r_.currentBitMap[index]
	shift := rowNum % 8
	return (byte((flag >> shift)) & (bitMap & byte(0xff))) == 0
}

func (r_ *IoTDBRpcDataSet) IsNullByIndex(columnIndex int) bool {
	index := r_.columnOrdinalDict[r_.findColumnNameByIndex(columnIndex)] - start_index
	// time column will never be None
	if index < 0 {
		return true
	}
	return r_.IsNull(index, r_.rowsIndex-1)
}

func (r_ *IoTDBRpcDataSet) IsNullByName(columnName string) bool {
	// will return 0 (default int) if columnOrdinalDict without this name
	index := r_.columnOrdinalDict[columnName] - start_index
	// time column will never be None
	if index < 0 {
		return true
	}
	return r_.IsNull(index, r_.rowsIndex-1)
}

func (r_ *IoTDBRpcDataSet) findColumnNameByIndex(columnIndex int) string {
	if columnIndex <= 0 {
		panic("Column index should start from 1")
	}
	if columnIndex > len(r_.columnNameList) {
		panic(fmt.Sprintf("Column index {%v} out of range {%v}\n", columnIndex, r_.columnSize))
	}
	return r_.columnNameList[columnIndex-1]
}

func (r_ *IoTDBRpcDataSet) fetchResults() bool {
	r_.rowsIndex = 0
	request := &rpc.TSFetchResultsReq{SessionId: r_.sessionId, Statement: r_.sql, FetchSize: r_.fetchSize, QueryId: r_.queryId, IsAlign: true}
	response, _ := r_.client.FetchResults(context.Background(), request)
	if !response.HasResultSet {
		r_.emptyResultSet = true
	} else {
		r_.emptyResultSet = false
		r_.queryDataSet = response.QueryDataSet
	}
	return response.HasResultSet
}

func (r_ *IoTDBRpcDataSet) ConstructOneRow() {
	r_.timeBytes = r_.queryDataSet.Time[:8]
	r_.queryDataSet.Time = r_.queryDataSet.Time[8:]
	for k, bitMap_buffer := range r_.queryDataSet.GetBitmapList() {
		if r_.rowsIndex%8 == 0 {
			r_.currentBitMap[k] = bitMap_buffer[0]
			r_.queryDataSet.BitmapList[k] = bitMap_buffer[1:]
		}
		if !r_.IsNull(int32(k), r_.rowsIndex) {
			valueBuffer := r_.queryDataSet.GetValueList()[k]
			dataType := r_.columnTypeDeduplicatedList[k]
			switch dataType {
			case TSDataType.BOOLEAN:
				r_.value = append(r_.value, valueBuffer[:1])
				r_.queryDataSet.ValueList[k] = valueBuffer[1:]
			case TSDataType.INT32:
				r_.value = append(r_.value, valueBuffer[:4])
				r_.queryDataSet.ValueList[k] = valueBuffer[4:]
			case TSDataType.INT64:
				r_.value = append(r_.value, valueBuffer[:8])
				r_.queryDataSet.ValueList[k] = valueBuffer[8:]
			case TSDataType.FLOAT:
				r_.value = append(r_.value, valueBuffer[:4])
				r_.queryDataSet.ValueList[k] = valueBuffer[4:]
			case TSDataType.DOUBLE:
				r_.value = append(r_.value, valueBuffer[:8])
				r_.queryDataSet.ValueList[k] = valueBuffer[8:]
			case TSDataType.TEXT:
				var length int32
				buf := bytes.NewBuffer(valueBuffer[:4])
				err := binary.Read(buf, binary.BigEndian, &length)
				if err != nil {
					panic(fmt.Sprintln("binary.Read Error in ConstructOneRow!!", err))
				}
				r_.value = append(r_.value, valueBuffer[4:4+length])
				r_.queryDataSet.ValueList[k] = valueBuffer[4+length:]
			default:
				panic(fmt.Sprintf("Unsupported dataType {%v}\n", dataType))
			}
		}
	}
	r_.rowsIndex += 1
	r_.hasCachedRecord = true
}

func (r_ *IoTDBRpcDataSet) GetFetchSize() int32 {
	return r_.fetchSize
}
func (r_ *IoTDBRpcDataSet) SetFetchSize(fetchSize int32) {
	r_.fetchSize = fetchSize
}

func (r_ *IoTDBRpcDataSet) GetColumnNames() []string {
	return r_.columnNameList
}

func (r_ *IoTDBRpcDataSet) GetColumnTypes() []int32 {
	return r_.columnTypeList
}

func (r_ *IoTDBRpcDataSet) GetColumnSize() int {
	return r_.columnSize
}

func (r_ *IoTDBRpcDataSet) GetIgnoreTimestamp() bool {
	return r_.ignoreTimestamp
}

func (r_ *IoTDBRpcDataSet) GetColumnOrdinalDict() map[string]int32 {
	return r_.columnOrdinalDict
}

func (r_ *IoTDBRpcDataSet) GetColumnTypeDeduplicatedList() []int32 {
	return r_.columnTypeDeduplicatedList
}

func (r_ *IoTDBRpcDataSet) GetValues() [][]byte {
	return r_.value
}

func (r_ *IoTDBRpcDataSet) GetTimeBytes() []byte {
	return r_.timeBytes
}

func (r_ *IoTDBRpcDataSet) GetHasCachedRecord() bool {
	return r_.hasCachedRecord
}
