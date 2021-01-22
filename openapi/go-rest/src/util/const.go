package util

// const in iotdb
const (
	SgPrefix = "system_p_sg"
	NodesSeparator = "."
	Wildcard = "*"
	Root = "root"
	Select = "select "
	From = " from "
	Where = " where "
	Time = " time "
	Ge = " >= "
	Le = " <= "
	And = " and "
	// When there is no label in some positions,this position will be replaced by ph
	Placeholder = "ph"
)

// const in prometheus
const (
	MetricKey = "__name__"
	StartTimeDeviation int64 = 300000
)

const (
	NullString = ""
)

