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
package org.apache.iotdb.isession.pool;

import org.apache.iotdb.isession.IDataIterator;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.ISessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.util.List;

public interface ISessionDataSetWrapper extends AutoCloseable {

  public ISession getSession();

  public int getBatchSize();

  public void setBatchSize(int batchSize);

  public boolean hasNext() throws IoTDBConnectionException, StatementExecutionException;

  public RowRecord next() throws IoTDBConnectionException, StatementExecutionException;

  /** retrieve data set like jdbc */
  public IDataIterator iterator();

  public List<String> getColumnNames();

  public List<String> getColumnTypes();

  public void close();

  public void setSession(ISession session);

  public ISessionDataSet getSessionDataSet();

  public void setSessionDataSet(ISessionDataSet sessionDataSet);
}
