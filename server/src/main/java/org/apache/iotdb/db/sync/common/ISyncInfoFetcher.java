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
package org.apache.iotdb.db.sync.common;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.qp.physical.sys.CreatePipePlan;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;
import org.apache.iotdb.db.sync.receiver.manager.PipeMessage;
import org.apache.iotdb.db.sync.sender.pipe.PipeInfo;
import org.apache.iotdb.db.sync.sender.pipe.PipeSink;

import java.util.List;

public interface ISyncInfoFetcher {

  // region Interfaces of PipeServer
  TSStatus startPipeServer();

  TSStatus stopPipeServer();

  boolean isPipeServerEnable();
  // endregion

  // region Interfaces of PipeSink
  // TODO: use CreatePipeSinkNode as parameter
  TSStatus addPipeSink(CreatePipeSinkPlan plan);

  TSStatus dropPipeSink(String name);

  PipeSink getPipeSink(String name);

  List<PipeSink> getAllPipeSink();
  // endregion

  // region Interfaces of Pipe

  // TODO: use CreatePipeNode as parameter
  TSStatus addPipe(CreatePipePlan plan, long createTime);

  TSStatus stopPipe(String pipeName);

  TSStatus startPipe(String pipeName);

  TSStatus dropPipe(String pipeName);

  List<PipeInfo> getAllPipeInfos();

  // endregion

  String getPipeMsg(String pipeName, long createTime);

  TSStatus recordMsg(String pipeName, long createTime, PipeMessage pipeMessage);
}
