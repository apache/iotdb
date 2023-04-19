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
package org.apache.iotdb.db.utils.sync;

import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.TsFilePipeInfo;
import org.apache.iotdb.commons.sync.pipesink.IoTDBPipeSink;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TPipeSinkInfo;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeSinkStatement;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.db.sync.sender.pipe.PipeSinkFactory;
import org.apache.iotdb.db.sync.sender.pipe.TsFilePipe;

public class SyncPipeUtil {

  public static PipeSink parseCreatePipeSinkStatement(
      CreatePipeSinkStatement createPipeSinkStatement) throws PipeSinkException {
    PipeSink pipeSink;
    try {
      pipeSink =
          PipeSinkFactory.createPipeSink(
              createPipeSinkStatement.getPipeSinkType(), createPipeSinkStatement.getPipeSinkName());
    } catch (UnsupportedOperationException e) {
      throw new PipeSinkException(e.getMessage());
    }

    pipeSink.setAttribute(createPipeSinkStatement.getAttributes());
    return pipeSink;
  }

  /** parse PipeInfo to Pipe, ignore status */
  public static Pipe parsePipeInfoAsPipe(PipeInfo pipeInfo, PipeSink pipeSink)
      throws PipeException {
    if (pipeInfo instanceof TsFilePipeInfo) {
      return new TsFilePipe(
          pipeInfo.getCreateTime(),
          pipeInfo.getPipeName(),
          pipeSink,
          ((TsFilePipeInfo) pipeInfo).getDataStartTimestamp(),
          ((TsFilePipeInfo) pipeInfo).isSyncDelOp());
    } else {
      throw new PipeException(String.format("Can not recognition pipeInfo type"));
    }
  }

  /** parse TPipeSinkInfo to PipeSink */
  public static PipeSink parseTPipeSinkInfoAsPipeSink(TPipeSinkInfo pipeSinkInfo)
      throws PipeSinkException {
    if (pipeSinkInfo.getPipeSinkType().equals(PipeSink.PipeSinkType.IoTDB.name())) {
      PipeSink pipeSink = new IoTDBPipeSink(pipeSinkInfo.getPipeSinkName());
      pipeSink.setAttribute(pipeSinkInfo.getAttributes());
      return pipeSink;
    } else {
      // TODO(ext-pipe): parse TPipeSinkInfo to external pipe sink
      throw new UnsupportedOperationException();
    }
  }

  /** parse TPipeInfo to PipeInfo */
  public static PipeInfo parseTCreatePipeReqAsPipeInfo(TCreatePipeReq pipeInfo, long pipeCreateTime)
      throws PipeException {
    return new TsFilePipeInfo();
  }
}
