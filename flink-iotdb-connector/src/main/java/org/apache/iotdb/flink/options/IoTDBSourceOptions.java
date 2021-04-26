/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.flink.options;

public class IoTDBSourceOptions extends IoTDBOptions {
  private String sql;
  private int fetchSize;

  public IoTDBSourceOptions(String host, int port, String user, String password, String sql) {
    this(host, port, user, password, sql, 1024);
  }

  public IoTDBSourceOptions(
      String host, int port, String user, String password, String sql, int fetchSIze) {
    super(host, port, user, password);
    this.sql = sql;
    this.fetchSize = fetchSIze;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  public void setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
  }
}
