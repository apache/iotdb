package cn.edu.tsinghua.tsfile.timeseries.filter.utils;

import cn.edu.tsinghua.tsfile.common.exception.filter.UnSupportFilterDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

import java.nio.ByteBuffer;

/**
 * @author JT
 * class to construct string digest.
 */
public class StrDigestForFilter extends DigestForFilter{
    private String min = null;
    private String max = null;
    private TSDataType type;

    public StrDigestForFilter(String min, String max, TSDataType type) {
        super();
        this.min = min;
        this.max = max;
        this.type = type;
    }

    public StrDigestForFilter(long minv, long maxv) {
        this.min = String.valueOf(minv);
        this.max = String.valueOf(maxv);
        this.type = TSDataType.INT64;
    }

    @SuppressWarnings("unchecked")
    public <T extends Comparable<T>> T getMinValue() {
        switch (type) {
            case INT32:
                return (T) ((Integer) Integer.parseInt(min));
            case INT64:
                return (T) ((Long) Long.parseLong(min));
            case FLOAT:
                return (T) ((Float) Float.parseFloat(min));
            case DOUBLE:
                return (T) ((Double) Double.parseDouble(min));
            case TEXT:
                return (T) new Binary(min);
            case BOOLEAN:
                return (T) ((Boolean) Boolean.parseBoolean(min));
            default:
                throw new UnSupportFilterDataTypeException("DigestForFilter unsupported datatype : " + type.toString());
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Comparable<T>> T getMaxValue() {
        switch (type) {
            case INT32:
                return (T) ((Integer) Integer.parseInt(max));
            case INT64:
                return (T) ((Long) Long.parseLong(max));
            case FLOAT:
                return (T) ((Float) Float.parseFloat(max));
            case DOUBLE:
                return (T) ((Double) Double.parseDouble(max));
            case TEXT:
                return (T) new Binary(max);
            case BOOLEAN:
                return (T) ((Boolean) Boolean.parseBoolean(max));
            default:
                throw new UnSupportFilterDataTypeException("DigestForFilter unsupported datatype : " + type.toString());
        }
    }

    public Class<?> getTypeClass() {
        switch (type) {
            case INT32:
                return Integer.class;
            case INT64:
                return Long.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case TEXT:
                return String.class;
            case BOOLEAN:
                return Boolean.class;
            default:
                throw new UnSupportFilterDataTypeException("DigestForFilter unsupported datatype : " + type.toString());
        }
    }

    public TSDataType getType() {
        return type;
    }
}
