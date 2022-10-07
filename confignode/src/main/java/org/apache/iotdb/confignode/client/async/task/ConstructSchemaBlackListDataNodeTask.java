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

package org.apache.iotdb.confignode.client.async.task;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.handlers.AbstractRetryHandler;
import org.apache.iotdb.confignode.client.async.handlers.ConstructSchemaBlackListHandler;

import java.util.Map;

public class ConstructSchemaBlackListDataNodeTask extends AbstractDataNodeTask<TSStatus> {

  public ConstructSchemaBlackListDataNodeTask(Map<Integer, TDataNodeLocation> dataNodeLocationMap) {
    super(dataNodeLocationMap);
  }

  @Override
  public DataNodeRequestType getDataNodeRequestType() {
    return DataNodeRequestType.CONSTRUCT_SCHEMA_BLACK_LIST;
  }

  @Override
  public AbstractRetryHandler getSingleRequestHandler() {
    return new ConstructSchemaBlackListHandler(dataNodeLocationMap, dataNodeResponseMap);
  }
}
