package cn.edu.tsinghua.iotdb.jdbc;

import cn.edu.tsinghua.service.rpc.thrift.TSDataValue;
import cn.edu.tsinghua.service.rpc.thrift.TSQueryDataSet;
import cn.edu.tsinghua.service.rpc.thrift.TSRowRecord;
import cn.edu.tsinghua.service.rpc.thrift.TS_Status;
import cn.edu.tsinghua.service.rpc.thrift.TS_StatusCode;
import cn.edu.tsinghua.tsfile.exception.write.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.read.common.Field;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.common.RowRecord;
import cn.edu.tsinghua.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utils to convert between thrift format and TsFile format
 */
public class Utils {

    /**
     * Parse JDBC connection URL The only supported format of the URL is:
     * jdbc:iotdb://localhost:6667/
     */
    public static IoTDBConnectionParams parseURL(String url, Properties info) throws IoTDBURLException {
        IoTDBConnectionParams params = new IoTDBConnectionParams(url);
        if (url.trim().equalsIgnoreCase(Config.IOTDB_URL_PREFIX)) {
            return params;
        }

        Pattern pattern = Pattern.compile("([^;]*):([^;]*)/");
        Matcher matcher = pattern.matcher(url.substring(Config.IOTDB_URL_PREFIX.length()));
        boolean isUrlLegal = false;
        while (matcher.find()) {
            params.setHost(matcher.group(1));
            params.setPort(Integer.parseInt((matcher.group(2))));
            isUrlLegal = true;
        }
        if (!isUrlLegal) {
            throw new IoTDBURLException("Error url format, url should be jdbc:iotdb://ip:port/");
        }

        if (info.containsKey(Config.AUTH_USER)) {
            params.setUsername(info.getProperty(Config.AUTH_USER));
        }
        if (info.containsKey(Config.AUTH_PASSWORD)) {
            params.setPassword(info.getProperty(Config.AUTH_PASSWORD));
        }

        return params;
    }

    public static void verifySuccess(TS_Status status) throws IoTDBSQLException {
        if (status.getStatusCode() != TS_StatusCode.SUCCESS_STATUS) {
            throw new IoTDBSQLException(status.errorMessage);
        }
    }
    
    public static List<RowRecord> convertRowRecords(TSQueryDataSet tsQueryDataSet) {
    		List<RowRecord> records = new ArrayList<>();
    		for(TSRowRecord ts : tsQueryDataSet.getRecords()) {
    			RowRecord r = new RowRecord(ts.getTimestamp());
    			int l = ts.getValuesSize();
    			for(int i = 0; i < l;i++) {
    				TSDataValue value = ts.getValues().get(i);
    				if(value.is_empty) {
    					Field field = new Field(null);
    					field.setNull();
    					r.getFields().add(field);
    				} else {
    					TSDataType dataType = TSDataType.valueOf(value.getType());
    					Field field = new Field(dataType);
					switch (dataType) {
					case BOOLEAN:
						field.setBoolV(value.isBool_val());
						break;
					case INT32:
						field.setIntV(value.getInt_val());
						break;
					case INT64:
						field.setLongV(value.getLong_val());
						break;
					case FLOAT:
						field.setFloatV((float)value.getFloat_val());
						break;
					case DOUBLE:
						field.setDoubleV(value.getDouble_val());
						break;
					case TEXT:
						field.setBinaryV(new Binary(value.getBinary_val()));;
						break;
					default:
						throw new UnSupportedDataTypeException(String.format("data type %s is not supported when convert data at client", dataType));
					}
					r.getFields().add(field);
    				}
    			}
    			records.add(r);
    		}
    		return records;
    }
}
