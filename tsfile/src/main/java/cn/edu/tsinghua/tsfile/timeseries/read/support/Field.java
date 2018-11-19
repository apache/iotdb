package cn.edu.tsinghua.tsfile.timeseries.read.support;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

/**
 * Field is the components of one {@code OldRowRecord} which store a value in
 * specific data type.
 *
 * @author Jinrui Zhang
 */
public class Field {

    public TSDataType dataType;
    public String deltaObjectId;
    public String measurementId;
    private boolean boolV;
    private int intV;
    private long longV;
    private float floatV;
    private double doubleV;
    private Binary binaryV;
    private boolean isNull;

    public Field(TSDataType dataType, String measurementId) {
        this.dataType = dataType;
        this.measurementId = measurementId;
        this.deltaObjectId = "default";
    }

    public Field(TSDataType dataType, String deltaObjectId, String measurementId) {
        this.dataType = dataType;
        this.deltaObjectId = deltaObjectId;
        this.measurementId = measurementId;
    }

    public boolean getBoolV() {
        return boolV;
    }

    public void setBoolV(boolean boolV) {
        this.boolV = boolV;
    }

    public int getIntV() {
        return intV;
    }

    public void setIntV(int intV) {
        this.intV = intV;
    }

    public long getLongV() {
        return longV;
    }

    public void setLongV(long longV) {
        this.longV = longV;
    }

    public float getFloatV() {
        return floatV;
    }

    public void setFloatV(float floatV) {
        this.floatV = floatV;
    }

    public double getDoubleV() {
        return doubleV;
    }

    public void setDoubleV(double doubleV) {
        this.doubleV = doubleV;
    }

    public Binary getBinaryV() {
        return binaryV;
    }

    public void setBinaryV(Binary binaryV) {
        this.binaryV = binaryV;
    }

    public String getStringValue() {
        if (isNull) {
            return "null";
        }
        switch (dataType) {
            case BOOLEAN:
                return String.valueOf(boolV);
            case INT32:
                return String.valueOf(intV);
            case INT64:
                return String.valueOf(longV);
            case FLOAT:
                return String.valueOf(floatV);
            case DOUBLE:
                return String.valueOf(doubleV);
            case TEXT:
                return binaryV.toString();
            case ENUMS:
                return binaryV.toString();
            default:
                throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
    }

    public String toString() {
        return getStringValue();
    }

    public boolean isNull() {
        return isNull;
    }

    public void setNull(boolean isNull) {
        this.isNull = isNull;
    }
}







