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

package org.apache.iotdb.subscription.api.dataset;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.List;

public interface ISubscriptionDataSet {
  Long getTime();

  List<String> getColumnTypes();

  List<String> getColumnNames();

  List<ByteBuffer> getDataResult();

  IDataGetter Data();

  interface IDataGetter {
    boolean getBoolean(int columnIndex);

    boolean getBoolean(String columnName);

    double getDouble(int columnIndex);

    double getDouble(String columnName);

    float getFloat(int columnIndex);

    float getFloat(String columnName);

    int getInt(int columnIndex);

    int getInt(String columnName);

    long getLong(int columnIndex);

    long getLong(String columnName);

    Object getObject(int columnIndex);

    Object getObject(String columnName);

    String getString(int columnIndex);

    String getString(String columnName);

    Timestamp getTimestamp(int columnIndex);

    Timestamp getTimestamp(String columnName);

    int findColumn(String columnName);
  }
}
