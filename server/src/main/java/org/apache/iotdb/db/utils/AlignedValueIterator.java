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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

/** Used for value object is instance of TsPrimitiveType[] */
public class AlignedValueIterator extends ValueIterator {

  int subMeasurementIndex;

  public AlignedValueIterator(Object[] values) {
    super(values);
  }

  public void setSubMeasurementIndex(int subMeasurementIndex) {
    this.subMeasurementIndex = subMeasurementIndex;
  }

  @Override
  public boolean hasNext() {
    while (curPos < values.length
        && (values[curPos] == null
            || ((TsPrimitiveType[]) values[curPos])[subMeasurementIndex] == null)) {
      curPos++;
    }
    return curPos < values.length;
  }

  @Override
  public Object next() {
    return ((TsPrimitiveType[]) values[curPos++])[subMeasurementIndex].getValue();
  }

  @Override
  public Object get(int index) {
    if (values[index] == null || ((TsPrimitiveType[]) values[index])[subMeasurementIndex] == null) {
      return null;
    }
    return ((TsPrimitiveType[]) values[index])[subMeasurementIndex].getValue();
  }
}
