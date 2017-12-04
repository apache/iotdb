package cn.edu.tsinghua.iotdb.service;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.iotdb.metadata.ColumnSchema;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TSColumnSchema;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TSDynamicOneColumnData;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TSQueryDataSet;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utils to convert between thrift format and TsFile format
 *
 */
public class Utils {

	private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

	public static Map<String, List<TSColumnSchema>> convertAllSchema(Map<String, List<ColumnSchema>> allSchema){
		if(allSchema == null){
			return null;
		}
		Map<String, List<TSColumnSchema>> tsAllSchema = new HashMap<>();
		for(Map.Entry<String, List<ColumnSchema>> entry : allSchema.entrySet()){
			List<TSColumnSchema> tsColumnSchemas = new ArrayList<>();
			for(ColumnSchema columnSchema : entry.getValue()){
				tsColumnSchemas.add(convertColumnSchema(columnSchema));
			}
			tsAllSchema.put(entry.getKey(), tsColumnSchemas);
		}
		return tsAllSchema;
	}
	
	private static TSColumnSchema convertColumnSchema(ColumnSchema schema){
		if(schema == null){
			return null;
		}
		TSColumnSchema tsColumnSchema = new TSColumnSchema();
		tsColumnSchema.setName(schema.name);
		tsColumnSchema.setDataType(schema.dataType == null ? null : schema.dataType.toString());
		tsColumnSchema.setEncoding(schema.encoding == null ? null : schema.encoding.toString());
		tsColumnSchema.setOtherArgs(schema.getArgsMap() == null ? null : schema.getArgsMap());
		return tsColumnSchema;
	}
	
	
	public static TSQueryDataSet convertQueryDataSet(QueryDataSet queryDataSet){
		List<String> keys = new ArrayList<>();
		List<TSDynamicOneColumnData> values = new ArrayList<>();
		for(Map.Entry<String,DynamicOneColumnData> entry: queryDataSet.mapRet.entrySet()){
			keys.add(entry.getKey());
			values.add(convertDynamicOneColumnData(entry.getValue()));
		}
		TSQueryDataSet tsQueryDataSet = new TSQueryDataSet(keys,values);
		return tsQueryDataSet;
	}
	
	
	private static TSDynamicOneColumnData convertDynamicOneColumnData(DynamicOneColumnData dynamicOneColumnData){

		//LOG.info("DynamicOneColumnData convert size : " +  dynamicOneColumnData.timeLength + "," + dynamicOneColumnData.emptyTimeLength);

		List<Long> timeRetList = new ArrayList<Long>();
		for(int i = 0 ; i < dynamicOneColumnData.timeLength; i ++){
				timeRetList.add(dynamicOneColumnData.getTime(i));
		}
		TSDynamicOneColumnData tsDynamicOneColumnData = new TSDynamicOneColumnData("", dynamicOneColumnData.dataType.toString(), dynamicOneColumnData.valueLength, timeRetList);

		List<Long> emptyTimeRetList = new ArrayList<>();
		if (dynamicOneColumnData.emptyTimeLength > 0) {
			for (int i = 0;i < dynamicOneColumnData.emptyTimeLength;i++) {
				emptyTimeRetList.add(dynamicOneColumnData.getEmptyTime(i));
			}
		}
		tsDynamicOneColumnData.setEmptyList(emptyTimeRetList);

		switch (dynamicOneColumnData.dataType) {
		case BOOLEAN:
			List<Boolean> boolList = new ArrayList<>();
			for(int i = 0 ; i < dynamicOneColumnData.valueLength; i ++){
					boolList.add(dynamicOneColumnData.getBoolean(i));
			}
			tsDynamicOneColumnData.setBoolList(boolList);
			break;
		case INT32:
			List<Integer> intList = new ArrayList<>();
			for(int i = 0 ; i < dynamicOneColumnData.valueLength; i ++){
					intList.add(dynamicOneColumnData.getInt(i));
			}
			tsDynamicOneColumnData.setI32List(intList);
			break;
		case INT64:
			List<Long> longList = new ArrayList<>();
			for(int i = 0 ; i < dynamicOneColumnData.valueLength; i ++){
					longList.add(dynamicOneColumnData.getLong(i));
			}
			tsDynamicOneColumnData.setI64List(longList);
			break;
		case FLOAT:
			List<Double> floatList = new ArrayList<>();
			for(int i = 0 ; i < dynamicOneColumnData.valueLength; i ++){
					floatList.add((double) dynamicOneColumnData.getFloat(i));
			}
			tsDynamicOneColumnData.setFloatList(floatList);
			break;
		case DOUBLE:
			List<Double> doubleList = new ArrayList<>();
			for(int i = 0 ; i < dynamicOneColumnData.valueLength; i ++){
					doubleList.add(dynamicOneColumnData.getDouble(i));
			}
			tsDynamicOneColumnData.setDoubleList(doubleList);
			break;
		case TEXT:
			List<ByteBuffer> binaryList = new ArrayList<>();
			for(int i = 0 ; i < dynamicOneColumnData.valueLength; i ++){
			    		binaryList.add(ByteBuffer.wrap(dynamicOneColumnData.getBinary(i).values));
			}
			tsDynamicOneColumnData.setBinaryList(binaryList);
			break;
		default:
		    	throw new UnSupportedDataTypeException(String.format("data type %s is not supported when convert data at server", dynamicOneColumnData.dataType));
		}
		
		return tsDynamicOneColumnData;
	}	
}
