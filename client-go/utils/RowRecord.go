package utils

import "fmt"

type RowRecord struct {
	timestamp int64
	fieldList []Field
}

func NewRowRecord(timestamp int64, fieldList []Field) *RowRecord {
	r_ := &RowRecord{}
	r_.timestamp = timestamp
	r_.fieldList = fieldList
	return r_
}

func (r_ *RowRecord) AddField(f Field) {
	r_.fieldList = append(r_.fieldList, f)
}

func (r_ *RowRecord) AddFieldWithValue(dataType int32, value interface{}) {
	r_.fieldList = append(r_.fieldList, *NewField(dataType, value))
}

func (r_ *RowRecord) ToString() string {
	str := string(r_.timestamp)
	for _, v := range r_.fieldList {
		str += "\t\t" + v.ToString()
	}
	return str
}

func (r_ *RowRecord) SetTimestamp(timestamp int64) {
	r_.timestamp = timestamp
}

func (r_ *RowRecord) GetTimestamp() int64 {
	return r_.timestamp
}

func (r_ *RowRecord) SetField(index int, field Field) bool {
	if index < len(r_.fieldList) {
		r_.fieldList[index] = field
		return true
	}
	fmt.Println("Out of FieldList Length!")
	return false
}

func (r_ *RowRecord) SetFields(fields []Field) {
	r_.fieldList = fields
}

func (r_ *RowRecord) GetFields() []Field {
	return r_.fieldList
}
