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
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.handlers.AbstractRetryHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractDataNodeTask<T> {

  protected Map<Integer, TDataNodeLocation> dataNodeLocationMap;

  protected Map<Integer, T> dataNodeResponseMap = new ConcurrentHashMap<>();

  /** Must provide thread-safe map */
  public AbstractDataNodeTask(Map<Integer, TDataNodeLocation> dataNodeLocationMap) {
    this.dataNodeLocationMap = dataNodeLocationMap;
  }

  public Map<Integer, TDataNodeLocation> getDataNodeLocationMap() {
    return dataNodeLocationMap;
  }

  public abstract DataNodeRequestType getDataNodeRequestType();

  /** Provide the handler for single rpc process */
  public abstract AbstractRetryHandler getSingleRequestHandler();

  /** Get the final responses from all dataNodes */
  public Map<Integer, T> getDataNodeResponseMap() {
    return dataNodeResponseMap;
  }
}
