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

public class ValueIterator {

  // Object: TsPrimitiveType[] or common data type
  protected Object[] values;
  protected int curPos = 0;

  public ValueIterator(Object[] values) {
    this.values = values;
  }

  public boolean hasNext() {
    while (curPos < values.length && values[curPos] == null) {
      curPos++;
    }
    return curPos < values.length;
  }

  public void setSubMeasurementIndex(int subMeasurementIndex) {}

  public Object next() {
    return values[curPos++];
  }

  public Object get(int index) {
    return values[index];
  }

  public int getCurPos() {
    return curPos;
  }

  public void reset() {
    this.curPos = 0;
  }
}
