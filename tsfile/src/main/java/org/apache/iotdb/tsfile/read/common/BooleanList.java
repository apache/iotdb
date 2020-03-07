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

import java.util.Arrays;


public class BooleanList extends IoTDBArrayList {

  private boolean[][] elementData = new boolean[ARRAY_INIT_SIZE][];

  public BooleanList() {
    initInsideArray(0);
  }

  @Override
  public void put(boolean value) {
    ensureCapacityInternal();
    elementData[currentArrayIndex][currentInsideIndex++] = value;
    size++;
  }

  @Override
  public void fastPut(boolean value) {
    if (currentInsideIndex == INSIDE_ARRAY_INIT_SIZE) {
      currentArrayIndex++;
      currentInsideIndex = 0;
    }
    elementData[currentArrayIndex][currentInsideIndex++] = value;
    size++;
  }


  @Override
  public Object getValue(int currentReadIndex) {
    return elementData[currentReadIndex / INSIDE_ARRAY_INIT_SIZE]
        [currentReadIndex & (INSIDE_ARRAY_INIT_SIZE - 1)];
  }


  public boolean getOriginValue(int currentReadIndex) {
    return elementData[currentReadIndex / INSIDE_ARRAY_INIT_SIZE]
        [currentReadIndex & (INSIDE_ARRAY_INIT_SIZE - 1)];
  }

  @Override
  protected void initInsideArray(int index) {
    if (elementData[index] == null) {
      elementData[index] = new boolean[INSIDE_ARRAY_INIT_SIZE];
    }
  }

  @Override
  protected int getArrayLength() {
    return elementData.length;
  }

  @Override
  protected void growArray(int size) {
    elementData = Arrays.copyOf(elementData, size);
  }

}
