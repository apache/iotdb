package util

func RecoverSchema() {
	dataSet, _ := Session.ExecuteQueryStatement("select * from root.system_p.label_info", 1000)
	for {
		haseNext, _ := dataSet.Next()
		if haseNext {
			Timestamp++
			record, _ := dataSet.GetRowRecord()
			metricName := record.GetFields()[0].GetText()
			tagName := record.GetFields()[1].GetText()
			tagOrder := record.GetFields()[2].GetInt32()

			if MetricTagOrder[metricName] == nil {
				MetricTagOrder[metricName] = make(map[string]int32)
				MetricOrderTag[metricName] = make(map[int32]string)
			}

			MetricTagOrder[metricName][tagName] = tagOrder
			MetricOrderTag[metricName][tagOrder] = tagName
		} else {
			break
		}
	}
}
