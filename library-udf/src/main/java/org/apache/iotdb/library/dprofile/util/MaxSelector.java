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
package org.apache.iotdb.library.dprofile.util;

/** Util for Mode */
public class MaxSelector {
  int times;
  int intValue;
  long longValue;
  float floatValue;
  double doubleValue;

  public MaxSelector() {
    times = 0;
  }

  public void insert(int item, int cnt) {
    if (cnt > times) {
      times = cnt;
      intValue = item;
    }
  }

  public void insert(long item, int cnt) {
    if (cnt > times) {
      times = cnt;
      longValue = item;
    }
  }

  public void insert(float item, int cnt) {
    if (cnt > times) {
      times = cnt;
      floatValue = item;
    }
  }

  public void insert(double item, int cnt) {
    if (cnt > times) {
      times = cnt;
      doubleValue = item;
    }
  }

  public int getInt() {
    return intValue;
  }

  public long getLong() {
    return longValue;
  }

  public float getFloat() {
    return floatValue;
  }

  public double getDouble() {
    return doubleValue;
  }
}
