/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.db.rescon.PrimitiveArrayManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.rescon.PrimitiveArrayManager.ARRAY_SIZE;

public class TimAlignedTVList extends AlignedTVList implements TimSort {

  private long[][] sortedTimestamps;
  private long pivotTime;

  private int[][] sortedIndices;
  private int pivotIndex;

  TimAlignedTVList(List<TSDataType> types) {
    super(types);
  }

  @Override
  public TVList getTvListByColumnIndex(List<Integer> columnIndex, List<TSDataType> dataTypeList) {
    List<List<Object>> values = new ArrayList<>();
    List<List<BitMap>> bitMaps = null;
    for (int i = 0; i < columnIndex.size(); i++) {
      // columnIndex == -1 means querying a non-exist column, add null column here
      if (columnIndex.get(i) == -1) {
        values.add(null);
      } else {
        values.add(this.values.get(columnIndex.get(i)));
        if (this.bitMaps != null && this.bitMaps.get(columnIndex.get(i)) != null) {
          if (bitMaps == null) {
            bitMaps = new ArrayList<>(columnIndex.size());
            for (int j = 0; j < columnIndex.size(); j++) {
              bitMaps.add(null);
            }
          }
          bitMaps.set(i, this.bitMaps.get(columnIndex.get(i)));
        }
      }
    }
    TimAlignedTVList alignedTvList = new TimAlignedTVList(dataTypeList);
    alignedTvList.timestamps = this.timestamps;
    alignedTvList.indices = this.indices;
    alignedTvList.values = values;
    alignedTvList.bitMaps = bitMaps;
    alignedTvList.rowCount = this.rowCount;
    return alignedTvList;
  }

  @Override
  public AlignedTVList clone() {
    TimAlignedTVList cloneList = new TimAlignedTVList(dataTypes);
    cloneAs(cloneList);
    for (int[] indicesArray : indices) {
      cloneList.indices.add(cloneIndex(indicesArray));
    }
    for (int i = 0; i < values.size(); i++) {
      List<Object> columnValues = values.get(i);
      for (Object valueArray : columnValues) {
        cloneList.values.get(i).add(cloneValue(dataTypes.get(i), valueArray));
      }
      // clone bitmap in columnIndex
      if (bitMaps != null && bitMaps.get(i) != null) {
        List<BitMap> columnBitMaps = bitMaps.get(i);
        if (cloneList.bitMaps == null) {
          cloneList.bitMaps = new ArrayList<>(dataTypes.size());
          for (int j = 0; j < dataTypes.size(); j++) {
            cloneList.bitMaps.add(null);
          }
        }
        if (cloneList.bitMaps.get(i) == null) {
          List<BitMap> cloneColumnBitMaps = new ArrayList<>();
          for (BitMap bitMap : columnBitMaps) {
            cloneColumnBitMaps.add(bitMap == null ? null : bitMap.clone());
          }
          cloneList.bitMaps.set(i, cloneColumnBitMaps);
        }
      }
    }
    return cloneList;
  }

  @Override
  public void sort() {
    if (sortedTimestamps == null
        || sortedTimestamps.length < PrimitiveArrayManager.getArrayRowCount(rowCount)) {
      sortedTimestamps =
          (long[][]) PrimitiveArrayManager.createDataListsByType(TSDataType.INT64, rowCount);
    }
    if (sortedIndices == null
        || sortedIndices.length < PrimitiveArrayManager.getArrayRowCount(rowCount)) {
      sortedIndices =
          (int[][]) PrimitiveArrayManager.createDataListsByType(TSDataType.INT32, rowCount);
    }
    if (!sorted) {
      sort(0, rowCount);
    }
    clearSortedValue();
    clearSortedTime();
    sorted = true;
  }

  @Override
  public void tim_set(int src, int dest) {
    set(src, dest);
  }

  @Override
  protected void set(int src, int dest) {
    long srcT = getTime(src);
    int srcV = getValueIndex(src);
    set(dest, srcT, srcV);
  }

  @Override
  public void setFromSorted(int src, int dest) {
    set(
        dest,
        sortedTimestamps[src / ARRAY_SIZE][src % ARRAY_SIZE],
        sortedIndices[src / ARRAY_SIZE][src % ARRAY_SIZE]);
  }

  @Override
  public void setToSorted(int src, int dest) {
    sortedTimestamps[dest / ARRAY_SIZE][dest % ARRAY_SIZE] = getTime(src);
    sortedIndices[dest / ARRAY_SIZE][dest % ARRAY_SIZE] = getValueIndex(src);
  }

  @Override
  public void setPivotTo(int pos) {
    set(pos, pivotTime, pivotIndex);
  }

  @Override
  public void saveAsPivot(int pos) {
    pivotTime = getTime(pos);
    pivotIndex = getValueIndex(pos);
  }

  @Override
  public void clearSortedTime() {
    if (sortedTimestamps != null) {
      sortedTimestamps = null;
    }
  }

  @Override
  public void clearSortedValue() {
    if (sortedIndices != null) {
      sortedIndices = null;
    }
  }

  @Override
  public int compare(int idx1, int idx2) {
    long t1 = getTime(idx1);
    long t2 = getTime(idx2);
    return Long.compare(t1, t2);
  }

  @Override
  public void reverseRange(int lo, int hi) {
    hi--;
    while (lo < hi) {
      long loT = getTime(lo);
      int loV = getValueIndex(lo);
      long hiT = getTime(hi);
      int hiV = getValueIndex(hi);
      set(lo++, hiT, hiV);
      set(hi--, loT, loV);
    }
  }

  @Override
  public void clear() {
    super.clear();
    clearSortedTime();
    clearSortedValue();
  }

  public static TimAlignedTVList deserialize(DataInputStream stream) throws IOException {
    int dataTypeNum = stream.readInt();
    List<TSDataType> dataTypes = new ArrayList<>(dataTypeNum);
    int[] columnIndexArray = new int[dataTypeNum];
    for (int columnIndex = 0; columnIndex < dataTypeNum; ++columnIndex) {
      dataTypes.add(ReadWriteIOUtils.readDataType(stream));
      columnIndexArray[columnIndex] = columnIndex;
    }

    int rowCount = stream.readInt();
    // time
    long[] times = new long[rowCount];
    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
      times[rowIndex] = stream.readLong();
    }
    // read value and bitmap by column
    Object[] values = new Object[dataTypeNum];
    BitMap[] bitMaps = new BitMap[dataTypeNum];
    for (int columnIndex = 0; columnIndex < dataTypeNum; ++columnIndex) {
      BitMap bitMap = new BitMap(rowCount);
      Object valuesOfOneColumn;
      switch (dataTypes.get(columnIndex)) {
        case TEXT:
          Binary[] binaryValues = new Binary[rowCount];
          for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            binaryValues[rowIndex] = ReadWriteIOUtils.readBinary(stream);
            if (ReadWriteIOUtils.readBool(stream)) {
              bitMap.mark(rowIndex);
            }
          }
          valuesOfOneColumn = binaryValues;
          break;
        case FLOAT:
          float[] floatValues = new float[rowCount];
          for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            floatValues[rowIndex] = stream.readFloat();
            if (ReadWriteIOUtils.readBool(stream)) {
              bitMap.mark(rowIndex);
            }
          }
          valuesOfOneColumn = floatValues;
          break;
        case INT32:
          int[] intValues = new int[rowCount];
          for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            intValues[rowIndex] = stream.readInt();
            if (ReadWriteIOUtils.readBool(stream)) {
              bitMap.mark(rowIndex);
            }
          }
          valuesOfOneColumn = intValues;
          break;
        case INT64:
          long[] longValues = new long[rowCount];
          for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            longValues[rowIndex] = stream.readLong();
            if (ReadWriteIOUtils.readBool(stream)) {
              bitMap.mark(rowIndex);
            }
          }
          valuesOfOneColumn = longValues;
          break;
        case DOUBLE:
          double[] doubleValues = new double[rowCount];
          for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            doubleValues[rowIndex] = stream.readDouble();
            if (ReadWriteIOUtils.readBool(stream)) {
              bitMap.mark(rowIndex);
            }
          }
          valuesOfOneColumn = doubleValues;
          break;
        case BOOLEAN:
          boolean[] booleanValues = new boolean[rowCount];
          for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            booleanValues[rowIndex] = ReadWriteIOUtils.readBool(stream);
            if (ReadWriteIOUtils.readBool(stream)) {
              bitMap.mark(rowIndex);
            }
          }
          valuesOfOneColumn = booleanValues;
          break;
        default:
          throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
      }
      values[columnIndex] = valuesOfOneColumn;
      bitMaps[columnIndex] = bitMap;
    }

    TimAlignedTVList tvList = new TimAlignedTVList(dataTypes);
    tvList.putAlignedValues(times, values, bitMaps, columnIndexArray, 0, rowCount);
    return tvList;
  }
}
