package cn.edu.tsinghua.iotdb.jdbc;

import cn.edu.tsinghua.service.rpc.thrift.TSDataValue;
import cn.edu.tsinghua.service.rpc.thrift.TSQueryDataSet;
import cn.edu.tsinghua.service.rpc.thrift.TSRowRecord;
import cn.edu.tsinghua.service.rpc.thrift.TS_Status;
import cn.edu.tsinghua.service.rpc.thrift.TS_StatusCode;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;

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
    
    public static List<RowRecord> convertRowRecords(TSQueryDataSet tsQueryDataSet) {
    		List<RowRecord> records = new ArrayList<>();
    		for(TSRowRecord ts : tsQueryDataSet.getRecords()) {
    			RowRecord r = new RowRecord(ts.getTimestamp());
    			r.setFields(new LinkedHashMap<Path, TsPrimitiveType>());
    			int l = ts.getKeysSize();
    			for(int i = 0; i < l;i++) {
    				Path path = new Path(ts.getKeys().get(i));
    				if(ts.getValues().get(i).is_empty) {
    					r.getFields().put(path, null);
    				} else {
    					TSDataValue value = ts.getValues().get(i);
    					TSDataType dataType = TSDataType.valueOf(value.getType());
					switch (dataType) {
					case BOOLEAN:
						r.getFields().put(path, new TsPrimitiveType.TsBoolean(value.isBool_val()));
						break;
					case INT32:
						r.getFields().put(path, new TsPrimitiveType.TsInt(value.getInt_val()));
						break;
					case INT64:
						r.getFields().put(path, new TsPrimitiveType.TsLong(value.getLong_val()));
						break;
					case FLOAT:
						r.getFields().put(path, new TsPrimitiveType.TsFloat((float)value.getFloat_val()));
						break;
					case DOUBLE:
						r.getFields().put(path, new TsPrimitiveType.TsDouble(value.getDouble_val()));
						break;
					case TEXT:
						r.getFields().put(path, new TsPrimitiveType.TsBinary(new Binary((value.getBinary_val()))));
						break;
					default:
						throw new UnSupportedDataTypeException(String.format("data type %s is not supported when convert data at client", dataType));
					}
    				}
    			}
    			records.add(r);
    		}
    		return records;
    }
}
