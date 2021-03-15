package utils

import "fmt"

type Field struct {
	dataType int32
	filled   bool
	value    interface{}
}

func NewField(dataType int32, value interface{}) *Field {
	f := Field{}
	f.SetDataType(dataType)
	switch dataType {
	case TSDataType.BOOLEAN:
		if v, ok := value.(bool); ok {
			f.SetBooleanValue(v)
		} else {
			return nil
		}
	case TSDataType.INT32:
		if v, ok := value.(int32); ok {
			f.SetInt32Value(v)
		} else {
			return nil
		}
	case TSDataType.INT64:
		if v, ok := value.(int64); ok {
			f.SetInt64Value(v)
		} else {
			return nil
		}
	case TSDataType.FLOAT:
		if v, ok := value.(float32); ok {
			f.SetFloat32Value(v)
		} else {
			return nil
		}
	case TSDataType.DOUBLE:
		if v, ok := value.(float64); ok {
			f.SetFloat64Value(v)
		} else {
			return nil
		}
	case TSDataType.TEXT:
		if v, ok := value.(string); ok {
			f.SetStringValue(v)
		} else {
			return nil
		}
	default:
		return nil
	}
	return &f
}

func (f_ *Field) SetDataType(dataType int32) {
	f_.dataType = dataType
	f_.filled = true
}

func (f_ *Field) GetDataType() int32 {
	return f_.dataType
}

func (f_ *Field) IsNull() bool {
	return !f_.filled
}

func (f_ *Field) ToString() string {
	if f_.IsNull() {
		return ""
	}
	// convert interface{} value to string
	return fmt.Sprintf("%v", f_.value)
}

func (f_ *Field) GetValueOriginal() interface{} {
	return f_.value
}

func (f_ *Field) SetBooleanValue(v bool) {
	f_.value = v
}

func (f_ *Field) GetBooleanValue() *bool {
	if v, ok := f_.value.(bool); ok {
		return &v
	} else {
		return nil
	}
}

func (f_ *Field) SetInt32Value(v int32) {
	f_.value = v
}

func (f_ *Field) GetInt32Value() *int32 {
	if v, ok := f_.value.(int32); ok {
		return &v
	} else {
		return nil
	}
}

func (f_ *Field) SetInt64Value(v int64) {
	f_.value = v
}

func (f_ *Field) GetInt64Value() *int64 {
	if v, ok := f_.value.(int64); ok {
		return &v
	} else {
		return nil
	}
}

func (f_ *Field) SetFloat32Value(v float32) {
	f_.value = v
}

func (f_ *Field) GetFloat32Value() *float32 {
	if v, ok := f_.value.(float32); ok {
		return &v
	} else {
		return nil
	}
}

func (f_ *Field) SetFloat64Value(v float64) {
	f_.value = v
}

func (f_ *Field) GetFloat64Value() *float64 {
	if v, ok := f_.value.(float64); ok {
		return &v
	} else {
		return nil
	}
}

func (f_ *Field) SetStringValue(v string) {
	f_.value = v
}

func (f_ *Field) GetStringValue() *string {
	if v, ok := f_.value.(string); ok {
		return &v
	} else {
		return nil
	}
}
