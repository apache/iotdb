package cn.edu.tsinghua.iotdb.utils;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

/**
 * Created by zhangjinrui on 2018/1/25.
 */
public class PrimitiveArrayListFactory {

    public static PrimitiveArrayList getByDataType(TSDataType dataType) {
        switch (dataType) {
            case BOOLEAN:
                return new PrimitiveArrayList(boolean.class);
            case INT32:
                return new PrimitiveArrayList(int.class);
            case INT64:
                return new PrimitiveArrayList(long.class);
            case FLOAT:
                return new PrimitiveArrayList(float.class);
            case DOUBLE:
                return new PrimitiveArrayList(double.class);
            case TEXT:
                return new PrimitiveArrayList(Binary.class);
            default:
                throw new UnSupportedDataTypeException("DataType: " + dataType);
        }
    }
}
