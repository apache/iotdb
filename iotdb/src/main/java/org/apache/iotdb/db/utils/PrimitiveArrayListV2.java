/**
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
package org.apache.iotdb.db.utils;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.monitor.collector.MemTableWriteTimeCost;
import org.apache.iotdb.db.monitor.collector.MemTableWriteTimeCost.MemTableWriteTimeCostType;

public class PrimitiveArrayListV2 {

  private static final int MAX_SIZE_OF_ONE_ARRAY = 512;
  private static final int INITIAL_SIZE = 512;

  private Class clazz;
  private List<Object> values;
  private List<long[]> timestamps;

  private int totalDataNumber; // Total data number of all objects of current ArrayList
  private int currentArrayIndex; // current index of array
  private int offsetInCurrentArray; // current index of element in current array
  private int currentArraySize; // number of current arrays

  public PrimitiveArrayListV2(Class clazz) {
    this.clazz = clazz;
    values = new ArrayList<>();
    timestamps = new ArrayList<>();
    values.add(Array.newInstance(clazz, INITIAL_SIZE));
    timestamps.add(new long[INITIAL_SIZE]);
    totalDataNumber = 0;

    currentArrayIndex = 0;
    currentArraySize = INITIAL_SIZE;
    offsetInCurrentArray = -1;
  }

  private void checkCapacity(int aimSize) {
    if (currentArraySize < aimSize) {
      if (currentArraySize < MAX_SIZE_OF_ONE_ARRAY) {
//        long start = System.currentTimeMillis();
        // expand current Array
        int newCapacity = Math.min(MAX_SIZE_OF_ONE_ARRAY, currentArraySize * 2);
        values.set(currentArrayIndex,
            expandArray(values.get(currentArrayIndex), currentArraySize, newCapacity));
        timestamps.set(currentArrayIndex,
            (long[]) expandArray(timestamps.get(currentArrayIndex), currentArraySize, newCapacity));
        currentArraySize = newCapacity;
//        MemTableWriteTimeCost.getInstance().measure(MemTableWriteTimeCostType.CAPACITY_1, start);
      } else {
//        if (currentArrayIndex == values.size() - 1) {
          // add a new Array to the list
          values.add(Array.newInstance(clazz, INITIAL_SIZE));
          timestamps.add(new long[INITIAL_SIZE]);
//        }
        currentArrayIndex++;
        currentArraySize = timestamps.get(currentArrayIndex).length;
        offsetInCurrentArray = -1;
      }
    }
  }

  private Object expandArray(Object array, int preLentgh, int aimLength) {
    Class arrayClass = array.getClass().getComponentType();
    Object newArray = Array.newInstance(arrayClass, aimLength);
    System.arraycopy(array, 0, newArray, 0, preLentgh);
    return newArray;
  }

  public void putTimestamp(long timestamp, Object value) {
    checkCapacity(offsetInCurrentArray + 1 + 1);
    offsetInCurrentArray++;
    timestamps.get(currentArrayIndex)[offsetInCurrentArray] = timestamp;
    Array.set(values.get(currentArrayIndex), offsetInCurrentArray, value);
    totalDataNumber++;
  }

  public long getTimestamp(int index) {
    checkIndex(index);
    return timestamps.get(index / MAX_SIZE_OF_ONE_ARRAY)[index % MAX_SIZE_OF_ONE_ARRAY];
  }

  public Object getValue(int index) {
    checkIndex(index);
    return Array.get(values.get(index / MAX_SIZE_OF_ONE_ARRAY), index % MAX_SIZE_OF_ONE_ARRAY);
  }

  private void checkIndex(int index) {
    if (index < 0) {
      throw new NegativeArraySizeException("negetive array index:" + index);
    }
    if (index >= totalDataNumber) {
      throw new ArrayIndexOutOfBoundsException("index: " + index);
    }
  }

  public int getTotalDataNumber() {
    return totalDataNumber;
  }

  @Override
  public PrimitiveArrayListV2 clone() {
    PrimitiveArrayListV2 cloneList = new PrimitiveArrayListV2(clazz);
    cloneList.values.clear();
    cloneList.timestamps.clear();
    for (Object valueArray : values) {
      cloneList.values.add(cloneArray(valueArray, clazz));
    }
    for (Object timestampArray : timestamps) {
      cloneList.timestamps.add((long[]) cloneArray(timestampArray, long.class));
    }
    cloneList.totalDataNumber = totalDataNumber;
    cloneList.currentArrayIndex = currentArrayIndex;
    cloneList.offsetInCurrentArray = offsetInCurrentArray;
    cloneList.currentArraySize = currentArraySize;
    return cloneList;
  }

  private Object cloneArray(Object array, Class clazz) {
    Object cloneArray = Array.newInstance(clazz, Array.getLength(array));
    System.arraycopy(array, 0, cloneArray, 0, Array.getLength(array));
    return cloneArray;
  }

  public Class getClazz() {
    return clazz;
  }

  public void reset(){
    totalDataNumber = 0;
    currentArrayIndex = 0;
    offsetInCurrentArray = -1;
  }
}
