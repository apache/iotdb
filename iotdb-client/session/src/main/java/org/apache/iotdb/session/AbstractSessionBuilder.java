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

package org.apache.iotdb.session;

import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.util.Version;

import java.time.ZoneId;
import java.util.List;

public abstract class AbstractSessionBuilder {

  public String host = SessionConfig.DEFAULT_HOST;
  public int rpcPort = SessionConfig.DEFAULT_PORT;
  public String username = SessionConfig.DEFAULT_USER;
  public String pw = SessionConfig.DEFAULT_PASSWORD;
  public int fetchSize = SessionConfig.DEFAULT_FETCH_SIZE;
  public ZoneId zoneId = null;
  public int thriftDefaultBufferSize = SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY;
  public int thriftMaxFrameSize = SessionConfig.DEFAULT_MAX_FRAME_SIZE;
  // this field only take effect in write request, nothing to do with any other type requests,
  // like query, load and so on.
  // if set to true, it means that we may redirect the write request to its corresponding leader
  // if set to false, it means that we will only send write request to first available DataNode(it
  // may be changed while current DataNode is not available, for example, we may retry to connect
  // to another available DataNode)
  // so even if enableRedirection is set to false, we may also send write request to another
  // datanode while encountering retriable errors in current DataNode
  public boolean enableRedirection = SessionConfig.DEFAULT_REDIRECTION_MODE;
  public boolean enableRecordsAutoConvertTablet = SessionConfig.DEFAULT_RECORDS_AUTO_CONVERT_TABLET;
  public Version version = SessionConfig.DEFAULT_VERSION;
  public long timeOut = SessionConfig.DEFAULT_QUERY_TIME_OUT;

  // set to true, means that we will start a background thread to fetch all available (Status is
  // not Removing) datanodes in cluster, and these available nodes will be used in retrying stage
  public boolean enableAutoFetch = SessionConfig.DEFAULT_ENABLE_AUTO_FETCH;

  public boolean useSSL = false;
  public String trustStore;
  public String trustStorePwd;

  // max retry count, if set to 0, means that we won't do any retry
  // we can use any available DataNodes(fetched in background thread if enableAutoFetch is true,
  // or nodeUrls user specified) to retry, even if enableRedirection is false
  public int maxRetryCount = SessionConfig.MAX_RETRY_COUNT;

  public long retryIntervalInMs = SessionConfig.RETRY_INTERVAL_IN_MS;

  public List<String> nodeUrls = null;

  public String sqlDialect = SessionConfig.SQL_DIALECT;

  public String database;
}
