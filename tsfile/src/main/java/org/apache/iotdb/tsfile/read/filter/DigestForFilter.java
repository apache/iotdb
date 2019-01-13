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
package org.apache.iotdb.tsfile.read.filter;

import org.apache.iotdb.tsfile.exception.filter.UnSupportFilterDataTypeException;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.exception.filter.UnSupportFilterDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.nio.ByteBuffer;

/**
 * class to construct digest.
 */
public class DigestForFilter {

    private ByteBuffer minValue;
    private ByteBuffer maxValue;
    private long minTime;
    private long maxTime;
    private TSDataType type;

    public DigestForFilter(long minTime, long maxTime, ByteBuffer minValue, ByteBuffer maxValue, TSDataType type) {
        this.minTime = minTime;
        this.maxTime = maxTime;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.type = type;
    }

    public DigestForFilter(long minTime, long maxTime, byte[] minValue, byte[] maxValue, TSDataType type) {
        this.minTime = minTime;
        this.maxTime = maxTime;
        this.minValue = ByteBuffer.wrap(minValue);
        this.maxValue = ByteBuffer.wrap(maxValue);
        this.type = type;
    }

    @SuppressWarnings("unchecked")
    private <T extends Comparable<T>> T getValue(ByteBuffer value) {
        switch (type) {
        case INT32:
            return (T) ((Integer) BytesUtils.bytesToInt(value.array()));
        case INT64:
            return (T) ((Long) BytesUtils.bytesToLong(value.array()));
        case FLOAT:
            return (T) ((Float) BytesUtils.bytesToFloat(value.array()));
        case DOUBLE:
            return (T) ((Double) BytesUtils.bytesToDouble(value.array()));
        case TEXT:
            return (T) new Binary(BytesUtils.bytesToString(value.array()));
        case BOOLEAN:
            return (T) (Boolean) BytesUtils.bytesToBool(value.array());
        default:
            throw new UnSupportFilterDataTypeException("DigestForFilter unsupported datatype : " + type.toString());
        }
    }

    public long getMinTime() {
        return minTime;
    }

    public long getMaxTime() {
        return maxTime;
    }

    public <T extends Comparable<T>> T getMinValue() {
        return getValue(minValue);
    }

    public <T extends Comparable<T>> T getMaxValue() {
        return getValue(maxValue);
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
