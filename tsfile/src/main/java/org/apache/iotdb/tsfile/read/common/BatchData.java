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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;

/**
 * <p>
 * <code>BatchData</code> is a self-defined data structure which is optimized for different type of values. This class
 * can be viewed as a collection which is more efficient than ArrayList.
 */
public class BatchData {

    private int TIME_CAPACITY = 1;
    private int VALUE_CAPACITY = 1;
    private int EMPTY_TIME_CAPACITY = 1;
    private int CAPACITY_THRESHOLD = 1024;

    private TSDataType dataType;
    private int curIdx;

    private int timeArrayIdx; // the number of ArrayList in timeRet
    private int curTimeIdx; // the index of current ArrayList in timeRet
    private int timeLength; // the insert timestamp number of timeRet

    private int valueArrayIdx;// the number of ArrayList in valueRet
    private int curValueIdx; // the index of current ArrayList in valueRet
    private int valueLength; // the insert value number of valueRet

    private ArrayList<long[]> timeRet;
    private ArrayList<long[]> emptyTimeRet;
    private ArrayList<boolean[]> booleanRet;
    private ArrayList<int[]> intRet;
    private ArrayList<long[]> longRet;
    private ArrayList<float[]> floatRet;
    private ArrayList<double[]> doubleRet;
    private ArrayList<Binary[]> binaryRet;

    public BatchData() {
        dataType = null;
    }

    public BatchData(TSDataType type) {
        dataType = type;
    }

    /**
     * @param type
     *            Data type to record for this BatchData
     * @param recordTime
     *            whether to record time value for this BatchData
     */
    public BatchData(TSDataType type, boolean recordTime) {
        init(type, recordTime, false);
    }

    public BatchData(TSDataType type, boolean recordTime, boolean hasEmptyTime) {
        init(type, recordTime, hasEmptyTime);
    }

    public boolean hasNext() {
        return curIdx < timeLength;
    }

    public void next() {
        curIdx++;
    }

    public long currentTime() {
        rangeCheckForTime(curIdx);
        return this.timeRet.get(curIdx / TIME_CAPACITY)[curIdx % TIME_CAPACITY];
    }

    public Object currentValue() {
        switch (dataType) {
        case INT32:
            return getInt();
        case INT64:
            return getLong();
        case FLOAT:
            return getFloat();
        case DOUBLE:
            return getDouble();
        case BOOLEAN:
            return getBoolean();
        case TEXT:
            return getBinary();
        default:
            return null;
        }
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public void init(TSDataType type, boolean recordTime, boolean hasEmptyTime) {
        this.dataType = type;
        this.valueArrayIdx = 0;
        this.curValueIdx = 0;
        this.valueLength = 0;
        this.curIdx = 0;
        CAPACITY_THRESHOLD = TSFileConfig.dynamicDataSize;

        if (recordTime) {
            timeRet = new ArrayList<>();
            timeRet.add(new long[TIME_CAPACITY]);
            timeArrayIdx = 0;
            curTimeIdx = 0;
            timeLength = 0;
        }

        if (hasEmptyTime) {
            emptyTimeRet = new ArrayList<>();
            emptyTimeRet.add(new long[EMPTY_TIME_CAPACITY]);
        }

        switch (dataType) {
        case BOOLEAN:
            booleanRet = new ArrayList<>();
            booleanRet.add(new boolean[VALUE_CAPACITY]);
            break;
        case INT32:
            intRet = new ArrayList<>();
            intRet.add(new int[VALUE_CAPACITY]);
            break;
        case INT64:
            longRet = new ArrayList<>();
            longRet.add(new long[VALUE_CAPACITY]);
            break;
        case FLOAT:
            floatRet = new ArrayList<>();
            floatRet.add(new float[VALUE_CAPACITY]);
            break;
        case DOUBLE:
            doubleRet = new ArrayList<>();
            doubleRet.add(new double[VALUE_CAPACITY]);
            break;
        case TEXT:
            binaryRet = new ArrayList<>();
            binaryRet.add(new Binary[VALUE_CAPACITY]);
            break;
        default:
            throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
    }

    public void putTime(long v) {
        if (curTimeIdx == TIME_CAPACITY) {
            if (TIME_CAPACITY >= CAPACITY_THRESHOLD) {
                this.timeRet.add(new long[TIME_CAPACITY]);
                timeArrayIdx++;
                curTimeIdx = 0;
            } else {
                long[] newData = new long[TIME_CAPACITY * 2];
                System.arraycopy(timeRet.get(0), 0, newData, 0, TIME_CAPACITY);
                this.timeRet.set(0, newData);
                TIME_CAPACITY = TIME_CAPACITY * 2;
            }
        }
        (timeRet.get(timeArrayIdx))[curTimeIdx++] = v;
        timeLength++;
    }

    public void putBoolean(boolean v) {
        if (curValueIdx == VALUE_CAPACITY) {
            if (VALUE_CAPACITY >= CAPACITY_THRESHOLD) {
                if (this.booleanRet.size() <= valueArrayIdx + 1) {
                    this.booleanRet.add(new boolean[VALUE_CAPACITY]);
                }
                valueArrayIdx++;
                curValueIdx = 0;
            } else {
                boolean[] newData = new boolean[VALUE_CAPACITY * 2];
                System.arraycopy(booleanRet.get(0), 0, newData, 0, VALUE_CAPACITY);
                this.booleanRet.set(0, newData);
                VALUE_CAPACITY = VALUE_CAPACITY * 2;
            }
        }
        (this.booleanRet.get(valueArrayIdx))[curValueIdx++] = v;
        valueLength++;
    }

    public void putInt(int v) {
        if (curValueIdx == VALUE_CAPACITY) {
            if (VALUE_CAPACITY >= CAPACITY_THRESHOLD) {
                if (this.intRet.size() <= valueArrayIdx + 1) {
                    this.intRet.add(new int[VALUE_CAPACITY]);
                }
                valueArrayIdx++;
                curValueIdx = 0;
            } else {
                int[] newData = new int[VALUE_CAPACITY * 2];
                System.arraycopy(intRet.get(0), 0, newData, 0, VALUE_CAPACITY);
                this.intRet.set(0, newData);
                VALUE_CAPACITY = VALUE_CAPACITY * 2;
            }
        }
        (this.intRet.get(valueArrayIdx))[curValueIdx++] = v;
        valueLength++;
    }

    public void putLong(long v) {
        if (curValueIdx == VALUE_CAPACITY) {
            if (VALUE_CAPACITY >= CAPACITY_THRESHOLD) {
                if (this.longRet.size() <= valueArrayIdx + 1) {
                    this.longRet.add(new long[VALUE_CAPACITY]);
                }
                valueArrayIdx++;
                curValueIdx = 0;
            } else {
                long[] newData = new long[VALUE_CAPACITY * 2];
                System.arraycopy(longRet.get(0), 0, newData, 0, VALUE_CAPACITY);
                this.longRet.set(0, newData);
                VALUE_CAPACITY = VALUE_CAPACITY * 2;
            }
        }
        (this.longRet.get(valueArrayIdx))[curValueIdx++] = v;
        valueLength++;
    }

    public void putFloat(float v) {
        if (curValueIdx == VALUE_CAPACITY) {
            if (VALUE_CAPACITY >= CAPACITY_THRESHOLD) {
                if (this.floatRet.size() <= valueArrayIdx + 1) {
                    this.floatRet.add(new float[VALUE_CAPACITY]);
                }
                valueArrayIdx++;
                curValueIdx = 0;
            } else {
                float[] newData = new float[VALUE_CAPACITY * 2];
                System.arraycopy(floatRet.get(0), 0, newData, 0, VALUE_CAPACITY);
                this.floatRet.set(0, newData);
                VALUE_CAPACITY = VALUE_CAPACITY * 2;
            }
        }
        (this.floatRet.get(valueArrayIdx))[curValueIdx++] = v;
        valueLength++;
    }

    public void putDouble(double v) {
        if (curValueIdx == VALUE_CAPACITY) {
            if (VALUE_CAPACITY >= CAPACITY_THRESHOLD) {
                if (this.doubleRet.size() <= valueArrayIdx + 1) {
                    this.doubleRet.add(new double[VALUE_CAPACITY]);
                }
                valueArrayIdx++;
                curValueIdx = 0;
            } else {
                double[] newData = new double[VALUE_CAPACITY * 2];
                System.arraycopy(doubleRet.get(0), 0, newData, 0, VALUE_CAPACITY);
                this.doubleRet.set(0, newData);
                VALUE_CAPACITY = VALUE_CAPACITY * 2;
            }
        }
        (this.doubleRet.get(valueArrayIdx))[curValueIdx++] = v;
        valueLength++;
    }

    public void putBinary(Binary v) {
        if (curValueIdx == VALUE_CAPACITY) {
            if (VALUE_CAPACITY >= CAPACITY_THRESHOLD) {
                if (this.binaryRet.size() <= valueArrayIdx + 1) {
                    this.binaryRet.add(new Binary[VALUE_CAPACITY]);
                }
                valueArrayIdx++;
                curValueIdx = 0;
            } else {
                Binary[] newData = new Binary[VALUE_CAPACITY * 2];
                System.arraycopy(binaryRet.get(0), 0, newData, 0, VALUE_CAPACITY);
                this.binaryRet.set(0, newData);
                VALUE_CAPACITY = VALUE_CAPACITY * 2;
            }
        }
        (this.binaryRet.get(valueArrayIdx))[curValueIdx++] = v;
        valueLength++;
    }

    /**
     * Checks if the given index is in range. If not, throws an appropriate runtime exception.
     */
    private void rangeCheck(int idx) {
        if (idx < 0) {
            throw new IndexOutOfBoundsException("BatchData value range check, Index is negative: " + idx);
        }
        if (idx >= valueLength) {
            throw new IndexOutOfBoundsException(
                    "BatchData value range check, Index : " + idx + ". Length : " + valueLength);
        }
    }

    /**
     * Checks if the given index is in range. If not, throws an appropriate runtime exception.
     */
    private void rangeCheckForTime(int idx) {
        if (idx < 0) {
            throw new IndexOutOfBoundsException("BatchData time range check, Index is negative: " + idx);
        }
        if (idx >= timeLength) {
            throw new IndexOutOfBoundsException(
                    "BatchData time range check, Index : " + idx + ". Length : " + timeLength);
        }
    }

    private void rangeCheckForEmptyTime(int idx) {
        if (idx < 0) {
            throw new IndexOutOfBoundsException("BatchData empty time range check, Index is negative: " + idx);
        }
    }

    public boolean getBoolean() {
        rangeCheck(curIdx);
        return this.booleanRet.get(curIdx / TIME_CAPACITY)[curIdx % TIME_CAPACITY];
    }

    public void setBoolean(int idx, boolean v) {
        rangeCheck(idx);
        this.booleanRet.get(idx / TIME_CAPACITY)[idx % TIME_CAPACITY] = v;
    }

    public int getInt() {
        rangeCheck(curIdx);
        return this.intRet.get(curIdx / TIME_CAPACITY)[curIdx % TIME_CAPACITY];
    }

    public void setInt(int idx, int v) {
        rangeCheck(idx);
        this.intRet.get(idx / TIME_CAPACITY)[idx % TIME_CAPACITY] = v;
    }

    public long getLong() {
        rangeCheck(curIdx);
        return this.longRet.get(curIdx / TIME_CAPACITY)[curIdx % TIME_CAPACITY];
    }

    public void setLong(int idx, long v) {
        rangeCheck(idx);
        this.longRet.get(idx / TIME_CAPACITY)[idx % TIME_CAPACITY] = v;
    }

    public float getFloat() {
        rangeCheck(curIdx);
        return this.floatRet.get(curIdx / TIME_CAPACITY)[curIdx % TIME_CAPACITY];
    }

    public void setFloat(int idx, float v) {
        rangeCheck(idx);
        this.floatRet.get(idx / TIME_CAPACITY)[idx % TIME_CAPACITY] = v;
    }

    public double getDouble() {
        rangeCheck(curIdx);
        return this.doubleRet.get(curIdx / TIME_CAPACITY)[curIdx % TIME_CAPACITY];
    }

    public void setDouble(int idx, double v) {
        rangeCheck(idx);
        this.doubleRet.get(idx / TIME_CAPACITY)[idx % TIME_CAPACITY] = v;
    }

    public Binary getBinary() {
        rangeCheck(curIdx);
        return this.binaryRet.get(curIdx / TIME_CAPACITY)[curIdx % TIME_CAPACITY];
    }

    public void setBinary(int idx, Binary v) {
        this.binaryRet.get(idx / TIME_CAPACITY)[idx % TIME_CAPACITY] = v;
    }

    public void setTime(int idx, long v) {
        rangeCheckForTime(idx);
        this.timeRet.get(idx / TIME_CAPACITY)[idx % TIME_CAPACITY] = v;
    }

    public long getEmptyTime(int idx) {
        rangeCheckForEmptyTime(idx);
        return this.emptyTimeRet.get(idx / EMPTY_TIME_CAPACITY)[idx % EMPTY_TIME_CAPACITY];
    }

    public long[] getTimeAsArray() {
        long[] res = new long[timeLength];
        for (int i = 0; i < timeLength; i++) {
            res[i] = timeRet.get(i / TIME_CAPACITY)[i % TIME_CAPACITY];
        }
        return res;
    }

    public void putAnObject(Object v) {
        switch (dataType) {
        case BOOLEAN:
            putBoolean((boolean) v);
            break;
        case INT32:
            putInt((int) v);
            break;
        case INT64:
            putLong((long) v);
            break;
        case FLOAT:
            putFloat((float) v);
            break;
        case DOUBLE:
            putDouble((double) v);
            break;
        case TEXT:
            putBinary((Binary) v);
            break;
        default:
            throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
    }

    public Comparable<?> getAnObject() {
        switch (dataType) {
        case BOOLEAN:
            return getBoolean();
        case DOUBLE:
            return getDouble();
        case TEXT:
            return getBinary();
        case FLOAT:
            return getFloat();
        case INT32:
            return getInt();
        case INT64:
            return getLong();
        default:
            throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
    }

    public void setAnObject(int idx, Comparable<?> v) {
        switch (dataType) {
        case BOOLEAN:
            setBoolean(idx, (Boolean) v);
            break;
        case DOUBLE:
            setDouble(idx, (Double) v);
            break;
        case TEXT:
            setBinary(idx, (Binary) v);
            break;
        case FLOAT:
            setFloat(idx, (Float) v);
            break;
        case INT32:
            setInt(idx, (Integer) v);
            break;
        case INT64:
            setLong(idx, (Long) v);
            break;
        default:
            throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
    }

    public int length() {
        return this.timeLength;
    }
}
