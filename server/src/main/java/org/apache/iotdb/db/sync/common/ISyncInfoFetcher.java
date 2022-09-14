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
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeSinkStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeStatement;
import org.apache.iotdb.db.qp.physical.sys.CreatePipePlan;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;
import org.apache.iotdb.db.sync.sender.pipe.PipeInfo;
import org.apache.iotdb.db.sync.sender.pipe.PipeMessage;
import org.apache.iotdb.db.sync.sender.pipe.PipeSink;

import java.util.List;

public interface ISyncInfoFetcher {

  // region Interfaces of PipeSink
  // TODO(sync): delete this in new-standalone version
  TSStatus addPipeSink(CreatePipeSinkPlan plan);

  TSStatus addPipeSink(CreatePipeSinkStatement createPipeSinkStatement);

  TSStatus dropPipeSink(String name);

  PipeSink getPipeSink(String name);

  List<PipeSink> getAllPipeSinks();
  // endregion

  // region Interfaces of Pipe

  // TODO(sync): delete this in new-standalone version
  TSStatus addPipe(CreatePipePlan plan, long createTime);

  TSStatus addPipe(CreatePipeStatement createPipeStatement, long createTime);

  TSStatus stopPipe(String pipeName);

  TSStatus startPipe(String pipeName);

  TSStatus dropPipe(String pipeName);

  List<PipeInfo> getAllPipeInfos();

  PipeInfo getRunningPipeInfo();

  // endregion

  String getPipeMsg(String pipeName, long createTime);

  TSStatus recordMsg(String pipeName, long createTime, PipeMessage pipeMessage);
}
