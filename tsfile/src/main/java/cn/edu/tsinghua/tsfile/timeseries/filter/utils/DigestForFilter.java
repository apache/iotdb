package cn.edu.tsinghua.tsfile.timeseries.filter.utils;

import cn.edu.tsinghua.tsfile.common.exception.filter.UnSupportFilterDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

import java.nio.ByteBuffer;

/**
 * @author ZJR
 * class to construct digest.
 */
public class DigestForFilter {

    private ByteBuffer min = null;
    private ByteBuffer max = null;
    private TSDataType type;

    public DigestForFilter() {

    }

    public DigestForFilter(ByteBuffer min, ByteBuffer max, TSDataType type) {
        this.min = min;
        this.max = max;
        this.type = type;
    }

    public DigestForFilter(long minv, long maxv) {
        this.min = ByteBuffer.wrap(BytesUtils.longToBytes(minv));
        this.max = ByteBuffer.wrap(BytesUtils.longToBytes(maxv));
        this.type = TSDataType.INT64;
    }

    @SuppressWarnings("unchecked")
    public <T extends Comparable<T>> T getMinValue() {
        switch (type) {
            case INT32:
                return (T) ((Integer) BytesUtils.bytesToInt(min.array()));
            case INT64:
                return (T) ((Long) BytesUtils.bytesToLong(min.array()));
            case FLOAT:
                return (T) ((Float) BytesUtils.bytesToFloat(min.array()));
            case DOUBLE:
                return (T) ((Double) BytesUtils.bytesToDouble(min.array()));
            case TEXT:
                return (T) new Binary(BytesUtils.bytesToString(min.array()));
            case BOOLEAN:
                return (T) (Boolean) BytesUtils.bytesToBool(min.array());
            default:
                throw new UnSupportFilterDataTypeException("DigestForFilter unsupported datatype : " + type.toString());
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Comparable<T>> T getMaxValue() {
        switch (type) {
            case INT32:
                return (T) ((Integer) BytesUtils.bytesToInt(max.array()));
            case INT64:
                return (T) ((Long) BytesUtils.bytesToLong(max.array()));
            case FLOAT:
                return (T) ((Float) BytesUtils.bytesToFloat(max.array()));
            case DOUBLE:
                return (T) ((Double) BytesUtils.bytesToDouble(max.array()));
            case TEXT:
                return (T) new Binary(BytesUtils.bytesToString(max.array()));
            case BOOLEAN:
                return (T) (Boolean) BytesUtils.bytesToBool(max.array());
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
