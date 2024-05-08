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
package org.apache.iotdb.session.pool;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.SessionDataSet.DataIterator;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.util.List;

public class SessionDataSetWrapper implements AutoCloseable {

  SessionDataSet sessionDataSet;
  Session session;
  SessionPool pool;

  public SessionDataSetWrapper(SessionDataSet sessionDataSet, Session session, SessionPool pool) {
    this.sessionDataSet = sessionDataSet;
    this.session = session;
    this.pool = pool;
  }

  protected Session getSession() {
    return session;
  }

  public int getBatchSize() {
    return sessionDataSet.getFetchSize();
  }

  public void setBatchSize(int batchSize) {
    sessionDataSet.setFetchSize(batchSize);
  }

  /**
   * If there is an Exception, and you do not want to use the resultset anymore, you have to release
   * the resultset manually by calling closeResultSet
   *
   * @return
   * @throws IoTDBConnectionException
   * @throws StatementExecutionException
   */
  public boolean hasNext() throws IoTDBConnectionException, StatementExecutionException {
    boolean next = sessionDataSet.hasNext();
    if (!next) {
      pool.closeResultSet(this);
    }
    return next;
  }
  /**
   * If there is an Exception, and you do not want to use the resultset anymore, you have to release
   * the resultset manually by calling closeResultSet
   *
   * @return
   * @throws IoTDBConnectionException
   * @throws StatementExecutionException
   */
  public RowRecord next() throws IoTDBConnectionException, StatementExecutionException {
    return sessionDataSet.next();
  }

  /** retrieve data set like jdbc */
  public DataIterator iterator() {
    return sessionDataSet.iterator();
  }

  public List<String> getColumnNames() {
    return sessionDataSet.getColumnNames();
  }

  public List<String> getColumnTypes() {
    return sessionDataSet.getColumnTypes();
  }

  /** close this dataset to release the session */
  @Override
  public void close() {
    pool.closeResultSet(this);
  }
}
