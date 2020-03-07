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
package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.utils.Binary;


public abstract class IoTDBArrayList {

  protected static final String ERR_DATATYPE_NOT_CONSISTENT = "DataType not consistent";
  protected static final int ARRAY_INIT_SIZE = 10;

  protected static final int POWER_N = 10;
  protected static final int INSIDE_ARRAY_INIT_SIZE = 2 << (POWER_N - 1);

  protected int currentArrayIndex = 0;
  protected int currentInsideIndex = 0;
  protected int size = 0;

  public abstract Object getValue(int currentReadIndex);


  public void ensureCapacity(int newSize) {
    int ideaCapacity = INSIDE_ARRAY_INIT_SIZE - currentInsideIndex;
    if (newSize <= ideaCapacity) {
      return;
    }
    int mallocSize = size + newSize;
    int newArraySize = (mallocSize >> POWER_N) + 1;

    if (newArraySize > getArrayLength()) {
      growArray(newArraySize);
    } else {
      newArraySize = getArrayLength() - currentInsideIndex;
    }

    // malloc for all inside array memory
    for (int i = currentArrayIndex + 1; i < newArraySize; i++) {
      initInsideArray(i);
    }
  }


  protected void ensureCapacityInternal() {
    if (INSIDE_ARRAY_INIT_SIZE - currentInsideIndex >= 1) {
      return;
    }

    currentArrayIndex++;
    if (currentArrayIndex == getArrayLength()) {
      int oldCapacity = getArrayLength();
      int newCapacity = oldCapacity + (oldCapacity >> 1);
      growArray(newCapacity);
    }
    initInsideArray(currentArrayIndex);
    currentInsideIndex = 0;
    return;
  }

  protected abstract void initInsideArray(int index);

  protected abstract int getArrayLength();

  protected abstract void growArray(int size);

  public int size() {
    return size;
  }


  public void put(long value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void put(int value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void put(float value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);

  }

  public void put(double value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void put(Binary value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void put(boolean value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void fastPut(long value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void fastPut(int value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void fastPut(float value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void fastPut(double value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void fastPut(Binary value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void fastPut(boolean value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }
}
