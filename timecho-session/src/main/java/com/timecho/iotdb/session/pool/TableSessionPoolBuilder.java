/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License");
return this; you may not use this file except in compliance
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

package com.timecho.iotdb.session.pool;

import com.timecho.iotdb.isession.pool.ITableSessionPool;

import java.time.ZoneId;
import java.util.List;

public class TableSessionPoolBuilder extends org.apache.iotdb.session.pool.TableSessionPoolBuilder {
  @Override
  public ITableSessionPool build() {
    super.prepare();
    return new TableSessionPool(new SessionPool(this));
  }

  @Override
  public TableSessionPoolBuilder connectionTimeoutInMs(int connectionTimeoutInMs) {
    super.connectionTimeoutInMs(connectionTimeoutInMs);
    return this;
  }

  @Override
  public TableSessionPoolBuilder database(String database) {
    super.database(database);
    return this;
  }

  @Override
  public TableSessionPoolBuilder enableAutoFetch(boolean enableAutoFetch) {
    super.enableAutoFetch(enableAutoFetch);
    return this;
  }

  @Override
  public TableSessionPoolBuilder enableIoTDBRpcCompression(boolean enableCompression) {
    super.enableIoTDBRpcCompression(enableCompression);
    return this;
  }

  @Override
  public TableSessionPoolBuilder enableRedirection(boolean enableRedirection) {
    super.enableRedirection(enableRedirection);
    return this;
  }

  @Override
  public TableSessionPoolBuilder enableThriftCompression(boolean enableCompression) {
    super.enableThriftCompression(enableCompression);
    return this;
  }

  @Override
  public TableSessionPoolBuilder fetchSize(int fetchSize) {
    super.fetchSize(fetchSize);
    return this;
  }

  @Override
  public TableSessionPoolBuilder maxRetryCount(int maxRetryCount) {
    super.maxRetryCount(maxRetryCount);
    return this;
  }

  @Override
  public TableSessionPoolBuilder maxSize(int maxSize) {
    super.maxSize(maxSize);
    return this;
  }

  @Override
  public TableSessionPoolBuilder nodeUrls(List<String> nodeUrls) {
    super.nodeUrls(nodeUrls);
    return this;
  }

  @Override
  public TableSessionPoolBuilder password(String password) {
    super.password(password);
    return this;
  }

  @Override
  public TableSessionPoolBuilder queryTimeoutInMs(long queryTimeoutInMs) {
    super.queryTimeoutInMs(queryTimeoutInMs);
    return this;
  }

  @Override
  public TableSessionPoolBuilder retryIntervalInMs(long retryIntervalInMs) {
    super.retryIntervalInMs(retryIntervalInMs);
    return this;
  }

  @Override
  public TableSessionPoolBuilder thriftDefaultBufferSize(int thriftDefaultBufferSize) {
    super.thriftDefaultBufferSize(thriftDefaultBufferSize);
    return this;
  }

  @Override
  public TableSessionPoolBuilder thriftMaxFrameSize(int thriftMaxFrameSize) {
    super.thriftMaxFrameSize(thriftMaxFrameSize);
    return this;
  }

  @Override
  public TableSessionPoolBuilder trustStore(String trustStore) {
    super.trustStore(trustStore);
    return this;
  }

  @Override
  public TableSessionPoolBuilder trustStorePwd(String trustStorePwd) {
    super.trustStorePwd(trustStorePwd);
    return this;
  }

  @Override
  public TableSessionPoolBuilder keyStore(String keyStore) {
    super.keyStore(keyStore);
    return this;
  }

  @Override
  public TableSessionPoolBuilder keyStorePwd(String keyStorePwd) {
    super.keyStorePwd(keyStorePwd);
    return this;
  }

  @Override
  public TableSessionPoolBuilder sslProtocol(String sslProtocol) {
    super.sslProtocol(sslProtocol);
    return this;
  }

  @Override
  public TableSessionPoolBuilder user(String user) {
    super.user(user);
    return this;
  }

  @Override
  public TableSessionPoolBuilder useSSL(boolean useSSL) {
    super.useSSL(useSSL);
    return this;
  }

  @Override
  public TableSessionPoolBuilder waitToGetSessionTimeoutInMs(long waitToGetSessionTimeoutInMs) {
    super.waitToGetSessionTimeoutInMs(waitToGetSessionTimeoutInMs);
    return this;
  }

  @Override
  public TableSessionPoolBuilder zoneId(ZoneId zoneId) {
    super.zoneId(zoneId);
    return this;
  }
}
