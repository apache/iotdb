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
package org.apache.iotdb.isession;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import java.sql.Timestamp;

public interface IDataIterator {

  boolean next() throws StatementExecutionException, IoTDBConnectionException;

  boolean isNull(int columnIndex) throws StatementExecutionException;

  boolean isNull(String columnName) throws StatementExecutionException;

  boolean getBoolean(int columnIndex) throws StatementExecutionException;

  boolean getBoolean(String columnName) throws StatementExecutionException;

  double getDouble(int columnIndex) throws StatementExecutionException;

  double getDouble(String columnName) throws StatementExecutionException;

  float getFloat(int columnIndex) throws StatementExecutionException;

  float getFloat(String columnName) throws StatementExecutionException;

  int getInt(int columnIndex) throws StatementExecutionException;

  int getInt(String columnName) throws StatementExecutionException;

  long getLong(int columnIndex) throws StatementExecutionException;

  long getLong(String columnName) throws StatementExecutionException;

  Object getObject(int columnIndex) throws StatementExecutionException;

  Object getObject(String columnName) throws StatementExecutionException;

  String getString(int columnIndex) throws StatementExecutionException;

  String getString(String columnName) throws StatementExecutionException;

  Timestamp getTimestamp(int columnIndex) throws StatementExecutionException;

  Timestamp getTimestamp(String columnName) throws StatementExecutionException;

  int findColumn(String columnName);
}
