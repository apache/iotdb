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

package org.apache.iotdb.db.utils.windowing.api;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

public interface Window {

  int size();

  TSDataType getDataType();

  long getTime(int index);

  int getInt(int index);

  long getLong(int index);

  float getFloat(int index);

  double getDouble(int index);

  boolean getBoolean(int index);

  Binary getBinary(int index);

  long[] getTimeArray();

  int[] getIntArray();

  long[] getLongArray();

  float[] getFloatArray();

  double[] getDoubleArray();

  boolean[] getBooleanArray();

  Binary[] getBinaryArray();

  void setInt(int index, int value);

  void setLong(int index, long value);

  void setFloat(int index, float value);

  void setDouble(int index, double value);

  void setBoolean(int index, boolean value);

  void setBinary(int index, Binary value);
}
