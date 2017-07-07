package cn.edu.thu.tsfiledb.query.aggregation;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;

public class AggregationResult {
	
	/*
	 * We use the DataStructure -> DynamicOneColumnData to store the aggregation value.
	 * And in general, only one data point can be used whose index is 0 in this DynamicOneColumnData
	 * to store the aggregation value.
	 * 
	 * And we make following rules: using the time whose index is 0 to represents the count of
	 * the records calculated; using the value whose index is 0 to represents the aggregation value.
	 */
	public DynamicOneColumnData data;
	
	AggregationResult(TSDataType dataType){
		data = new DynamicOneColumnData(dataType, true);
		data.setDeltaObjectType("Aggregation");
	}
	
}
