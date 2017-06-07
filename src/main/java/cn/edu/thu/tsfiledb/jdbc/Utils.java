package cn.edu.thu.tsfiledb.jdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cn.edu.thu.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.thu.tsfile.common.utils.Binary;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.metadata.ColumnSchema;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSColumnSchema;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSDynamicOneColumnData;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSQueryDataSet;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TS_Status;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TS_StatusCode;

/**
 * Utils to convert between thrift format and TsFile format
 *
 */
public class Utils {

    /**
     * Parse JDBC connection URL The only supported format of the URL is:
     * jdbc:tsfile://localhost:6667/
     */
    public static TsfileConnectionParams parseURL(String url, Properties info) throws TsfileURLException {
	TsfileConnectionParams params = new TsfileConnectionParams(url);
	if (url.trim().equalsIgnoreCase(TsfileJDBCConfig.TSFILE_URL_PREFIX)) {
	    return params;
	}

	Pattern pattern = Pattern.compile("([^;]*):([^;]*)/");
	Matcher matcher = pattern.matcher(url.substring(TsfileJDBCConfig.TSFILE_URL_PREFIX.length()));
	boolean isUrlLegal = false;
	while (matcher.find()) {
	    params.setHost(matcher.group(1));
	    params.setPort(Integer.parseInt((matcher.group(2))));
	    isUrlLegal = true;
	}
	if (!isUrlLegal) {
	    throw new TsfileURLException("Error url format, url should be jdbc:tsfile://ip:port/");
	}

	if (info.containsKey(TsfileJDBCConfig.AUTH_USER)) {
	    params.setUsername(info.getProperty(TsfileJDBCConfig.AUTH_USER));
	}
	if (info.containsKey(TsfileJDBCConfig.AUTH_PASSWORD)) {
	    params.setPassword(info.getProperty(TsfileJDBCConfig.AUTH_PASSWORD));
	}

	return params;
    }

    public static void verifySuccess(TS_Status status) throws TsfileSQLException {
	if (status.getStatusCode() != TS_StatusCode.SUCCESS_STATUS) {
	    throw new TsfileSQLException(status.errorMessage);
	}
    }

    public static Map<String, List<ColumnSchema>> convertAllSchema(Map<String, List<TSColumnSchema>> tsAllSchema) {
	if (tsAllSchema == null) {
	    return null;
	}
	Map<String, List<ColumnSchema>> allSchema = new HashMap<>();
	for (Map.Entry<String, List<TSColumnSchema>> entry : tsAllSchema.entrySet()) {
	    List<ColumnSchema> columnSchemas = new ArrayList<>();
	    for (TSColumnSchema columnSchema : entry.getValue()) {
		columnSchemas.add(convertColumnSchema(columnSchema));
	    }
	    allSchema.put(entry.getKey(), columnSchemas);
	}
	return allSchema;
    }

    private static ColumnSchema convertColumnSchema(TSColumnSchema tsSchema) {
	if (tsSchema == null) {
	    return null;
	}
	TSDataType dataType = tsSchema.dataType == null ? null : TSDataType.valueOf(tsSchema.dataType);
	TSEncoding encoding = tsSchema.encoding == null ? null : TSEncoding.valueOf(tsSchema.encoding);
	ColumnSchema ColumnSchema = new ColumnSchema(tsSchema.name, dataType, encoding);
	ColumnSchema.setArgsMap(tsSchema.getOtherArgs());
	return ColumnSchema;
    }

    public static QueryDataSet convertQueryDataSet(TSQueryDataSet tsQueryDataSet) {
	QueryDataSet queryDataSet = new QueryDataSet();
	List<String> keys = tsQueryDataSet.getKeys();
	List<TSDynamicOneColumnData> values = tsQueryDataSet.getValues();

	LinkedHashMap<String, DynamicOneColumnData> ret = new LinkedHashMap<>();
	int length = keys.size();
	for (int i = 0; i < length; i++) {
	    ret.put(keys.get(i), convertDynamicOneColumnData(values.get(i)));
	}
	queryDataSet.mapRet = ret;
	return queryDataSet;
    }

    private static DynamicOneColumnData convertDynamicOneColumnData(TSDynamicOneColumnData tsDynamicOneColumnData) {
	TSDataType dataType = TSDataType.valueOf(tsDynamicOneColumnData.getDataType());
	DynamicOneColumnData dynamicOneColumnData = new DynamicOneColumnData(dataType, true);
	dynamicOneColumnData.setDeltaObjectType(tsDynamicOneColumnData.getDeviceType());

	for (long time : tsDynamicOneColumnData.getTimeRet()) {
	    dynamicOneColumnData.putTime(time);
	}

	switch (dataType) {
	case BOOLEAN:
	    tsDynamicOneColumnData.getBoolList().forEach(dynamicOneColumnData::putBoolean);
	    break;
	case INT32:
	    tsDynamicOneColumnData.getI32List().forEach(dynamicOneColumnData::putInt);
	    break;
	case INT64:
	    tsDynamicOneColumnData.getI64List().forEach(dynamicOneColumnData::putLong);
	    break;
	case FLOAT:
	    List<Double> floats = tsDynamicOneColumnData.getFloatList();
	    for (double f : floats) {
		dynamicOneColumnData.putFloat((float) f);
	    }
	    break;
	case DOUBLE:
	    tsDynamicOneColumnData.getDoubleList().forEach(dynamicOneColumnData::putDouble);
	    break;
	case BYTE_ARRAY:
	    List<Byte> binaries = tsDynamicOneColumnData.getBinaryList();
	    for (Byte b : binaries) {
		dynamicOneColumnData.putBinary(new Binary(b.toString()));
	    }
	    break;
	default:
	    throw new UnSupportedDataTypeException(
		    String.format("data type %s is not supported when convert data at client", dataType));
	}
	return dynamicOneColumnData;
    }
}
