/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License");you may not use this file except in compliance
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

package com.timecho.iotdb.session;

import org.apache.iotdb.rpc.IoTDBConnectionException;

import com.timecho.iotdb.isession.ITableSession;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.time.ZoneId;
import java.util.List;

public class TableSessionBuilder extends org.apache.iotdb.session.TableSessionBuilder {

  @Override
  public TableSessionBuilder connectionTimeoutInMs(int connectionTimeoutInMs) {
    super.connectionTimeoutInMs(connectionTimeoutInMs);
    return this;
  }

  @Override
  public TableSessionBuilder database(String database) {
    super.database(database);
    return this;
  }

  @Override
  public TableSessionBuilder enableAutoFetch(boolean enableAutoFetch) {
    super.enableAutoFetch(enableAutoFetch);
    return this;
  }

  @Override
  public TableSessionBuilder enableCompaction(boolean enableCompaction) {
    super.enableCompaction(enableCompaction);
    return this;
  }

  @Override
  public TableSessionBuilder enableCompression(boolean enableCompression) {
    super.enableCompression(enableCompression);
    return this;
  }

  @Override
  public TableSessionBuilder enableRedirection(boolean enableRedirection) {
    super.enableRedirection(enableRedirection);
    return this;
  }

  @Override
  public TableSessionBuilder fetchSize(int fetchSize) {
    super.fetchSize(fetchSize);
    return this;
  }

  @Override
  public TableSessionBuilder maxRetryCount(int maxRetryCount) {
    super.maxRetryCount(maxRetryCount);
    return this;
  }

  @Override
  public TableSessionBuilder nodeUrls(List<String> nodeUrls) {
    super.nodeUrls(nodeUrls);
    return this;
  }

  @Override
  public TableSessionBuilder password(String password) {
    super.password(password);
    return this;
  }

  @Override
  public TableSessionBuilder queryTimeoutInMs(long queryTimeoutInMs) {
    super.queryTimeoutInMs(queryTimeoutInMs);
    return this;
  }

  @Override
  public TableSessionBuilder retryIntervalInMs(long retryIntervalInMs) {
    super.retryIntervalInMs(retryIntervalInMs);
    return this;
  }

  @Override
  public TableSessionBuilder tabletCompressionMinRowSize(int tabletCompressionMinRowSize) {
    super.tabletCompressionMinRowSize(tabletCompressionMinRowSize);
    return this;
  }

  @Override
  public TableSessionBuilder thriftDefaultBufferSize(int thriftDefaultBufferSize) {
    super.thriftDefaultBufferSize(thriftDefaultBufferSize);
    return this;
  }

  @Override
  public TableSessionBuilder thriftMaxFrameSize(int thriftMaxFrameSize) {
    super.thriftMaxFrameSize(thriftMaxFrameSize);
    return this;
  }

  @Override
  public TableSessionBuilder trustStore(String trustStore) {
    super.trustStore(trustStore);
    return this;
  }

  @Override
  public TableSessionBuilder trustStorePwd(String trustStorePwd) {
    super.trustStorePwd(trustStorePwd);
    return this;
  }

  @Override
  public TableSessionBuilder keyStore(String keyStore) {
    super.keyStore(keyStore);
    return this;
  }

  @Override
  public TableSessionBuilder keyStorePwd(String keyStorePwd) {
    super.keyStorePwd(keyStorePwd);
    return this;
  }

  @Override
  public TableSessionBuilder sslProtocol(String sslProtocol) {
    super.sslProtocol(sslProtocol);
    return this;
  }

  @Override
  public TableSessionBuilder username(String username) {
    super.username(username);
    return this;
  }

  @Override
  public TableSessionBuilder useSSL(boolean useSSL) {
    super.useSSL(useSSL);
    return this;
  }

  @Override
  public TableSessionBuilder withBlobEncoding(TSEncoding tsEncoding) {
    super.withBlobEncoding(tsEncoding);
    return this;
  }

  @Override
  public TableSessionBuilder withBooleanEncoding(TSEncoding tsEncoding) {
    super.withBooleanEncoding(tsEncoding);
    return this;
  }

  @Override
  public TableSessionBuilder withCompressionType(CompressionType compressionType) {
    super.withCompressionType(compressionType);
    return this;
  }

  @Override
  public TableSessionBuilder withDateEncoding(TSEncoding tsEncoding) {
    super.withDateEncoding(tsEncoding);
    return this;
  }

  @Override
  public TableSessionBuilder withDoubleEncoding(TSEncoding tsEncoding) {
    super.withDoubleEncoding(tsEncoding);
    return this;
  }

  @Override
  public TableSessionBuilder withFloatEncoding(TSEncoding tsEncoding) {
    super.withFloatEncoding(tsEncoding);
    return this;
  }

  @Override
  public TableSessionBuilder withInt32Encoding(TSEncoding tsEncoding) {
    super.withInt32Encoding(tsEncoding);
    return this;
  }

  @Override
  public TableSessionBuilder withInt64Encoding(TSEncoding tsEncoding) {
    super.withInt64Encoding(tsEncoding);
    return this;
  }

  @Override
  public TableSessionBuilder withStringEncoding(TSEncoding tsEncoding) {
    super.withStringEncoding(tsEncoding);
    return this;
  }

  @Override
  public TableSessionBuilder withTextEncoding(TSEncoding tsEncoding) {
    super.withTextEncoding(tsEncoding);
    return this;
  }

  @Override
  public TableSessionBuilder withTimeStampEncoding(TSEncoding tsEncoding) {
    super.withTimeStampEncoding(tsEncoding);
    return this;
  }

  @Override
  public TableSessionBuilder zoneId(ZoneId zoneId) {
    super.zoneId(zoneId);
    return this;
  }

  @Override
  public ITableSession build() throws IoTDBConnectionException {
    return new TableSession(constructNewSession());
  }
}
