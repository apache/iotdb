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

package org.apache.iotdb.db.query.udf.core.access;

import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.datastructure.tv.ElasticSerializableTVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;

public class ElasticSerializableTVListBackedSingleColumnRow implements Row {

  private final ElasticSerializableTVList tvList;
  private int currentRowIndex;

  public ElasticSerializableTVListBackedSingleColumnRow(
      ElasticSerializableTVList tvList, int currentRowIndex) {
    this.tvList = tvList;
    this.currentRowIndex = currentRowIndex;
  }

  @Override
  public long getTime() throws IOException {
    return tvList.getTime(currentRowIndex);
  }

  @Override
  public int getInt(int columnIndex) throws IOException {
    return tvList.getInt(currentRowIndex);
  }

  @Override
  public long getLong(int columnIndex) throws IOException {
    return tvList.getLong(currentRowIndex);
  }

  @Override
  public float getFloat(int columnIndex) throws IOException {
    return tvList.getFloat(currentRowIndex);
  }

  @Override
  public double getDouble(int columnIndex) throws IOException {
    return tvList.getDouble(currentRowIndex);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws IOException {
    return tvList.getBoolean(currentRowIndex);
  }

  @Override
  public Binary getBinary(int columnIndex) throws IOException {
    return tvList.getBinary(currentRowIndex);
  }

  @Override
  public String getString(int columnIndex) throws IOException {
    return tvList.getString(currentRowIndex);
  }

  @Override
  public TSDataType getDataType(int columnIndex) {
    return tvList.getDataType();
  }

  @Override
  public boolean isNull(int columnIndex) {
    return false;
  }

  @Override
  public int size() {
    return 1;
  }

  public Row seek(int currentRowIndex) {
    this.currentRowIndex = currentRowIndex;
    return this;
  }
}
