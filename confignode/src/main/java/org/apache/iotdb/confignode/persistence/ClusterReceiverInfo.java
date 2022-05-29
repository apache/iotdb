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
package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.read.ShowPipeReq;
import org.apache.iotdb.confignode.consensus.request.write.OperateReceiverPipeReq;
import org.apache.iotdb.confignode.consensus.response.PipeInfoResp;
import org.apache.iotdb.db.sync.receiver.AbstractReceiverInfo;
import org.apache.iotdb.db.sync.receiver.manager.PipeMessage;
import org.apache.iotdb.db.sync.receiver.recovery.ReceiverLog;
import org.apache.iotdb.db.sync.receiver.recovery.ReceiverLogAnalyzer;
import org.apache.iotdb.db.sync.sender.pipe.Pipe.PipeStatus;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterReceiverInfo extends AbstractReceiverInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterReceiverInfo.class);

  private boolean pipeServerEnable;
  // <pipeName, <remoteIp, <createTime, status>>>
  private Map<String, Map<String, Map<Long, PipeStatus>>> pipeInfos;
  // <pipeFolderName, pipeMsg>
  private Map<String, List<PipeMessage>> pipeMessageMap;
  private ReceiverLog log;

  public ClusterReceiverInfo() {
    log = new ReceiverLog();
    ReceiverLogAnalyzer analyzer = new ReceiverLogAnalyzer();
    try {
      analyzer.scan();
      pipeInfos = analyzer.getPipeInfos();
      pipeServerEnable = analyzer.isPipeServerEnable();
      pipeMessageMap = analyzer.getPipeMessageMap();
    } catch (StartupException e) {
      e.printStackTrace();
      pipeInfos = new ConcurrentHashMap<>();
      pipeMessageMap = new ConcurrentHashMap<>();
      pipeServerEnable = false;
    }
  }

  @Override
  protected void afterStartPipe(String pipeName, String remoteIp, long createTime) {}

  @Override
  protected void afterStopPipe(String pipeName, String remoteIp, long createTime) {}

  @Override
  protected void afterDropPipe(String pipeName, String remoteIp, long createTime) {}

  public synchronized TSStatus operatePipe(OperateReceiverPipeReq req) {
    try {
      switch (req.getOperateType()) {
        case HEARTBEAT:
          PipeMessage message =
              getPipeMessage(req.getPipeName(), req.getRemoteIp(), req.getCreateTime(), true);
          switch (message.getType()) {
            case INFO:
              break;
            case WARN:
              return new TSStatus(TSStatusCode.SYNC_RECEIVER_WARN.getStatusCode());
            case ERROR:
              return new TSStatus(TSStatusCode.SYNC_RECEIVER_ERROR.getStatusCode());
            default:
              throw new UnsupportedOperationException("Wrong message type " + message.getType());
          }
          break;
        case CREATE:
          createPipe(req.getPipeName(), req.getRemoteIp(), req.getCreateTime());
          break;
        case START:
          startPipe(req.getPipeName(), req.getRemoteIp(), req.getCreateTime());
          break;
        case STOP:
          stopPipe(req.getPipeName(), req.getRemoteIp(), req.getCreateTime());
          break;
        case DROP:
          dropPipe(req.getPipeName(), req.getRemoteIp(), req.getCreateTime());
          break;
      }
    } catch (IOException e) {
      return new TSStatus(TSStatusCode.PERSISTENCE_FAILURE.getStatusCode());
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public PipeInfoResp showPipe(ShowPipeReq req) {
    PipeInfoResp pipeInfoResp = new PipeInfoResp();
    if (StringUtils.isEmpty(req.getPipeName())) {
      pipeInfoResp.setPipeInfoList(getAllPipeInfos());
    } else {
      pipeInfoResp.setPipeInfoList(getPipeInfosByPipeName(req.getPipeName()));
    }
    pipeInfoResp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    return pipeInfoResp;
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    return false;
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {}
}
