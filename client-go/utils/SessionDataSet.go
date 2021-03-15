package utils

import (
	"bytes"
	"client-go/gen-go/rpc"
	"encoding/binary"
	"fmt"
)

type SessionDataSet struct {
	MyRpcDataSet *IoTDBRpcDataSet
}

func NewSessionDataSet(sql string, columnNameList []string, columnTypeList []int32, columnName2Index map[string]int32, queryId int64, client *rpc.TSIServiceClient, sessionId int64, queryDataSet *rpc.TSQueryDataSet, ignoreTimestamp bool) *SessionDataSet {
	return &SessionDataSet{MyRpcDataSet: NewIoTDBRpcDataSet(sql, columnNameList, columnTypeList, columnName2Index, ignoreTimestamp, queryId, client, sessionId, queryDataSet, 1024)}
}

func (s_ *SessionDataSet) GetFetchSize() int32 {
	return s_.MyRpcDataSet.fetchSize
}

func (s_ *SessionDataSet) SetFetchSize(fetchSize int32) {
	s_.MyRpcDataSet.SetFetchSize(fetchSize)
}

func (s_ *SessionDataSet) GetColumnNames() []string {
	return s_.GetColumnNames()
}

func (s_ *SessionDataSet) GetColumnTypes() []int32 {
	return s_.GetColumnTypes()
}

func (s_ *SessionDataSet) HasNext() bool {
	return s_.MyRpcDataSet.Next()
}

func (s_ *SessionDataSet) Next() *RowRecord {
	if !s_.MyRpcDataSet.GetHasCachedRecord() {
		if !s_.HasNext() {
			return nil
		}
	}
	s_.MyRpcDataSet.hasCachedRecord = false
	return s_.ConstructRowRecordFromValueArray()
}

func (s_ *SessionDataSet) ConstructRowRecordFromValueArray() *RowRecord {
	outFields := make([]Field, 0)
	for i := 0; i < s_.MyRpcDataSet.GetColumnSize(); i++ {
		index := i + 1
		datasetColumnIndex := i + int(start_index)
		if s_.MyRpcDataSet.GetIgnoreTimestamp() {
			index -= 1
			datasetColumnIndex -= 1
		}
		columnName := s_.MyRpcDataSet.GetColumnNames()[index]
		location := s_.MyRpcDataSet.GetColumnOrdinalDict()[columnName] - start_index

		field := Field{}
		if !s_.MyRpcDataSet.IsNullByIndex(datasetColumnIndex) {
			valueBytes := s_.MyRpcDataSet.GetValues()[location]
			dataType := s_.MyRpcDataSet.GetColumnTypeDeduplicatedList()[location]
			field.SetDataType(dataType)
			switch dataType {
			case TSDataType.BOOLEAN:
				value := new(bool)
				err := binary.Read(bytes.NewBuffer(valueBytes), binary.BigEndian, value)
				if err != nil {
					panic(fmt.Sprintf("binary.Read Error Occurred! {%v}\n", err))
					return nil
				}
				field.SetBooleanValue(*value)
			case TSDataType.INT32:
				value := new(int32)
				err := binary.Read(bytes.NewBuffer(valueBytes), binary.BigEndian, value)
				if err != nil {
					panic(fmt.Sprintf("binary.Read Error Occurred! {%v}\n", err))
					return nil
				}
				field.SetInt32Value(*value)
			case TSDataType.INT64:
				value := new(int64)
				err := binary.Read(bytes.NewBuffer(valueBytes), binary.BigEndian, value)
				if err != nil {
					panic(fmt.Sprintf("binary.Read Error Occurred! {%v}\n", err))
					return nil
				}
				field.SetInt64Value(*value)
			case TSDataType.FLOAT:
				value := new(float32)
				err := binary.Read(bytes.NewBuffer(valueBytes), binary.BigEndian, value)
				if err != nil {
					panic(fmt.Sprintf("binary.Read Error Occurred! {%v}\n", err))
					return nil
				}
				field.SetFloat32Value(*value)
			case TSDataType.DOUBLE:
				value := new(float64)
				err := binary.Read(bytes.NewBuffer(valueBytes), binary.BigEndian, value)
				if err != nil {
					panic(fmt.Sprintf("binary.Read Error Occurred! {%v}\n", err))
					return nil
				}
				field.SetFloat64Value(*value)
			case TSDataType.TEXT:
				// here may cause some problems, bytes or string?
				value := valueBytes
				field.SetStringValue(string(value))
			default:
				panic(fmt.Sprintf("Unsupported dataType {%v}\n", dataType))
			}
		} else {
		}
		outFields = append(outFields, field)

	}
	timestamp := new(int64)
	buf := bytes.NewBuffer(s_.MyRpcDataSet.GetTimeBytes())
	err := binary.Read(buf, binary.BigEndian, timestamp)
	if err != nil {
		panic(fmt.Sprintf("binary.Read Error Occurred! {%v}\n", err))
		return nil
	}
	return NewRowRecord(*timestamp, outFields)
}

func (s_ *SessionDataSet) CloseOperationHandle() {
	s_.MyRpcDataSet.Close()
}
