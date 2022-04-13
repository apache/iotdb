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

package org.apache.iotdb.db.mpp.execution.config;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.SetStorageGroupStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetStorageGroupTask implements IConfigTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(SetStorageGroupTask.class);
  private Statement statement;

  public SetStorageGroupTask(Statement statement) {
    this.statement = statement;
  }

  @Override
  public ListenableFuture<Void> execute() throws TException, MetadataException {
    if (!(statement instanceof SetStorageGroupStatement)) {
      LOGGER.error("SetStorageGroup not get SetStorageGroupStatement");
      return Futures.immediateVoidFuture();
    }
    // Construct request using statement
    SetStorageGroupStatement setStorageGroupStatement = (SetStorageGroupStatement) statement;
    TSetStorageGroupReq req =
        new TSetStorageGroupReq(setStorageGroupStatement.getStorageGroupPath().getFullPath());

    // Send request to some API server
    ConfigIService.Client client;
    // TODO SpriCoder borrow client

    // Get response or throw exception
    TSStatus tsStatus = client.setStorageGroup(req);
    if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
      throw new MetadataException("Failed to set storage Group");
    }
    // If the action is executed successfully, return the Future.
    // If your operation is async, you can return the corresponding future directly.
    return Futures.immediateVoidFuture();
  }
}
