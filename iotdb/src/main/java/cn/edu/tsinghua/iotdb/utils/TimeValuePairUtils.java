package cn.edu.tsinghua.iotdb.utils;

import cn.edu.tsinghua.tsfile.exception.write.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.read.common.BatchData;

public class TimeValuePairUtils {

    public static TimeValuePair getCurrentTimeValuePair(BatchData data) {
        switch (data.getDataType()) {
            case INT32:
                return new TimeValuePair(data.currentTime(), new TsPrimitiveType.TsInt(data.getInt()));
            case INT64:
                return new TimeValuePair(data.currentTime(), new TsPrimitiveType.TsLong(data.getLong()));
            case FLOAT:
                return new TimeValuePair(data.currentTime(), new TsPrimitiveType.TsFloat(data.getFloat()));
            case DOUBLE:
                return new TimeValuePair(data.currentTime(), new TsPrimitiveType.TsDouble(data.getDouble()));
            case TEXT:
                return new TimeValuePair(data.currentTime(), new TsPrimitiveType.TsBinary(data.getBinary()));
            case BOOLEAN:
                return new TimeValuePair(data.currentTime(), new TsPrimitiveType.TsBoolean(data.getBoolean()));
            default:
                throw new UnSupportedDataTypeException(String.valueOf(data.getDataType()));
        }
    }
}
