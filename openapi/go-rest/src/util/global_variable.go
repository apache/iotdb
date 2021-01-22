package util

import (
	"github.com/apache/iotdb-client-go/client"
)

var (
	Session        *client.Session
	DeviceId       = "root.system_p.label_info"
	MetricTagOrder = make(map[string]map[string]int32)
	Timestamp      int64
	Config         conf
	MetricOrderTag = make(map[string]map[int32]string)
)







