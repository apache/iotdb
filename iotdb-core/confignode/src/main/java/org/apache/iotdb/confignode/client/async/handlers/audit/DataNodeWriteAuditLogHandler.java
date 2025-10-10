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
package org.apache.iotdb.confignode.client.async.handlers.audit;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataNodeWriteAuditLogHandler implements AsyncMethodCallback<TSStatus> {
  private static final Logger logger = LoggerFactory.getLogger(DataNodeWriteAuditLogHandler.class);
  private final int nodeId;

  public DataNodeWriteAuditLogHandler(int nodeId) {
    this.nodeId = nodeId;
  }

  @Override
  public void onComplete(TSStatus tsStatus) {
    if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      logger.error("Failed to write audit log to DataNode {}, response: {}", nodeId, tsStatus);
    }
  }

  @Override
  public void onError(Exception e) {
    logger.error("Failed to write audit log to DataNode {}", nodeId, e);
  }
}
