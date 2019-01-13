/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * <p>
 * Field is component of one {@code RowRecord} which stores a value in specific data type. The value type of Field is
 * primitive(int long, float, double, binary, boolean).
 */
public class Field {

    private TSDataType dataType;
    private boolean boolV;
    private int intV;
    private long longV;
    private float floatV;
    private double doubleV;
    private Binary binaryV;
    private boolean isNull;

    public Field(TSDataType dataType) {
        this.dataType = dataType;
    }

    public TSDataType getDataType() {
        return dataType;
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
        if (isNull || dataType == null) {
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
        default:
            throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
    }

    @Override
    public String toString() {
        return getStringValue();
    }

    public void setNull() {
        this.isNull = true;
    }

    public boolean isNull() {
        return this.isNull;
    }
}
