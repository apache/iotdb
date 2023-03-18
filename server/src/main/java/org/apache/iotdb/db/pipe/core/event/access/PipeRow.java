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

package org.apache.iotdb.db.pipe.core.event.access;

import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;
import org.apache.iotdb.pipe.api.type.Binary;
import org.apache.iotdb.pipe.api.type.Type;
import org.apache.iotdb.tsfile.read.common.Path;

import java.io.IOException;
import java.util.List;

public class PipeRow implements Row {

  @Override
  public long getTime() throws IOException {
    return 0;
  }

  @Override
  public int getInt(int columnIndex) throws IOException {
    return 0;
  }

  @Override
  public long getLong(int columnIndex) throws IOException {
    return 0;
  }

  @Override
  public float getFloat(int columnIndex) throws IOException {
    return 0;
  }

  @Override
  public double getDouble(int columnIndex) throws IOException {
    return 0;
  }

  @Override
  public boolean getBoolean(int columnIndex) throws IOException {
    return false;
  }

  @Override
  public Binary getBinary(int columnIndex) throws IOException {
    return null;
  }

  @Override
  public String getString(int columnIndex) throws IOException {
    return null;
  }

  @Override
  public Type getDataType(int columnIndex) {
    return null;
  }

  @Override
  public boolean isNull(int columnIndex) {
    return false;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public int getColumnIndex(Path columnName) throws PipeParameterNotValidException {
    return 0;
  }

  @Override
  public List<Path> getColumnNames() {
    return null;
  }

  @Override
  public List<Type> getColumnTypes() {
    return null;
  }
}
