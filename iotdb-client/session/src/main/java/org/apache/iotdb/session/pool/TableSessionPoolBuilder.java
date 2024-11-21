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

import org.apache.iotdb.isession.pool.ITableSessionPool;

import java.time.ZoneId;
import java.util.List;

public class TableSessionPoolBuilder extends AbstractSessionPoolBuilder {

  public TableSessionPoolBuilder nodeUrls(List<String> nodeUrls) {
    this.nodeUrls = nodeUrls;
    return this;
  }

  public TableSessionPoolBuilder maxSize(int maxSize) {
    this.maxSize = maxSize;
    return this;
  }

  public TableSessionPoolBuilder user(String user) {
    this.username = user;
    return this;
  }

  public TableSessionPoolBuilder password(String password) {
    this.pw = password;
    return this;
  }

  public TableSessionPoolBuilder database(String database) {
    this.database = database;
    return this;
  }

  public TableSessionPoolBuilder queryTimeoutInMs(long queryTimeoutInMs) {
    this.timeOut = queryTimeoutInMs;
    return this;
  }

  public TableSessionPoolBuilder fetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
    return this;
  }

  public TableSessionPoolBuilder zoneId(ZoneId zoneId) {
    this.zoneId = zoneId;
    return this;
  }

  public TableSessionPoolBuilder waitToGetSessionTimeoutInMs(long waitToGetSessionTimeoutInMs) {
    this.waitToGetSessionTimeoutInMs = waitToGetSessionTimeoutInMs;
    return this;
  }

  public TableSessionPoolBuilder thriftDefaultBufferSize(int thriftDefaultBufferSize) {
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
    return this;
  }

  public TableSessionPoolBuilder thriftMaxFrameSize(int thriftMaxFrameSize) {
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    return this;
  }

  public TableSessionPoolBuilder enableCompression(boolean enableCompression) {
    this.enableCompression = enableCompression;
    return this;
  }

  public TableSessionPoolBuilder enableRedirection(boolean enableRedirection) {
    this.enableRedirection = enableRedirection;
    return this;
  }

  public TableSessionPoolBuilder connectionTimeoutInMs(int connectionTimeoutInMs) {
    this.connectionTimeoutInMs = connectionTimeoutInMs;
    return this;
  }

  public TableSessionPoolBuilder enableAutoFetch(boolean enableAutoFetch) {
    this.enableAutoFetch = enableAutoFetch;
    return this;
  }

  public TableSessionPoolBuilder maxRetryCount(int maxRetryCount) {
    this.maxRetryCount = maxRetryCount;
    return this;
  }

  public TableSessionPoolBuilder retryIntervalInMs(long retryIntervalInMs) {
    this.retryIntervalInMs = retryIntervalInMs;
    return this;
  }

  public TableSessionPoolBuilder useSSL(boolean useSSL) {
    this.useSSL = useSSL;
    return this;
  }

  public TableSessionPoolBuilder trustStore(String keyStore) {
    this.trustStore = keyStore;
    return this;
  }

  public TableSessionPoolBuilder trustStorePwd(String keyStorePwd) {
    this.trustStorePwd = keyStorePwd;
    return this;
  }

  public ITableSessionPool build() {
    if (nodeUrls == null) {
      throw new IllegalArgumentException("You should specify nodeUrls");
    }
    this.sqlDialect = "table";
    SessionPool sessionPool = new SessionPool(this);
    return new TableSessionPool(sessionPool);
  }
}
