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

package org.apache.iotdb.db.query.udf.api;

import java.io.IOException;
import org.apache.iotdb.tsfile.utils.Binary;

public interface DataPointCollector {

  void putInt(long timestamp, int value) throws IOException;

  void putLong(long timestamp, long value) throws IOException;

  void putFloat(long timestamp, float value) throws IOException;

  void putDouble(long timestamp, double value) throws IOException;

  void putBoolean(long timestamp, boolean value) throws IOException;

  void putBinary(long timestamp, Binary value) throws IOException;

  void putString(long timestamp, String value) throws IOException;
}
