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

import org.apache.iotdb.db.exception.sync.PipeException;
import org.apache.iotdb.db.exception.sync.PipeSinkException;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeSinkStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeStatement;
import org.apache.iotdb.db.qp.physical.sys.CreatePipePlan;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.db.sync.sender.pipe.PipeInfo;
import org.apache.iotdb.db.sync.sender.pipe.PipeSink;
import org.apache.iotdb.db.sync.sender.pipe.TsFilePipe;
import org.apache.iotdb.db.sync.sender.pipe.TsFilePipeInfo;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.Map;

public class SyncPipeUtil {

  // TODO(sync): delete this in new-standalone version
  public static PipeSink parseCreatePipeSinkPlan(CreatePipeSinkPlan plan) throws PipeSinkException {
    PipeSink pipeSink;
    try {
      pipeSink =
          PipeSink.PipeSinkFactory.createPipeSink(plan.getPipeSinkType(), plan.getPipeSinkName());
    } catch (UnsupportedOperationException e) {
      throw new PipeSinkException(e.getMessage());
    }

    pipeSink.setAttribute(plan.getPipeSinkAttributes());
    return pipeSink;
  }

  public static PipeSink parseCreatePipeSinkStatement(
      CreatePipeSinkStatement createPipeSinkStatement) throws PipeSinkException {
    PipeSink pipeSink;
    try {
      pipeSink =
          PipeSink.PipeSinkFactory.createPipeSink(
              createPipeSinkStatement.getPipeSinkType(), createPipeSinkStatement.getPipeSinkName());
    } catch (UnsupportedOperationException e) {
      throw new PipeSinkException(e.getMessage());
    }

    pipeSink.setAttribute(createPipeSinkStatement.getAttributes());
    return pipeSink;
  }

  // TODO(sync): delete this in new-standalone version
  public static Pipe parseCreatePipePlanAsPipe(
      CreatePipePlan plan, PipeSink pipeSink, long pipeCreateTime) throws PipeException {
    boolean syncDelOp = true;
    for (Pair<String, String> pair : plan.getPipeAttributes()) {
      pair.left = pair.left.toLowerCase();
      if ("syncdelop".equals(pair.left)) {
        syncDelOp = Boolean.parseBoolean(pair.right);
      } else {
        throw new PipeException(String.format("Can not recognition attribute %s", pair.left));
      }
    }

    return new TsFilePipe(
        pipeCreateTime, plan.getPipeName(), pipeSink, plan.getDataStartTimestamp(), syncDelOp);
  }

  public static Pipe parseCreatePipePlanAsPipe(
      CreatePipeStatement createPipeStatement, PipeSink pipeSink, long pipeCreateTime)
      throws PipeException {
    boolean syncDelOp = true;
    for (Map.Entry<String, String> entry : createPipeStatement.getPipeAttributes().entrySet()) {
      String attributeKey = entry.getKey().toLowerCase();
      if ("syncdelop".equals(attributeKey)) {
        syncDelOp = Boolean.parseBoolean(entry.getValue());
      } else {
        throw new PipeException(String.format("Can not recognition attribute %s", entry.getKey()));
      }
    }

    return new TsFilePipe(
        pipeCreateTime,
        createPipeStatement.getPipeName(),
        pipeSink,
        createPipeStatement.getStartTime(),
        syncDelOp);
  }

  // TODO(sync): delete this in new-standalone version
  public static PipeInfo parseCreatePipePlanAsPipeInfo(
      CreatePipePlan plan, PipeSink pipeSink, long pipeCreateTime) throws PipeException {
    boolean syncDelOp = true;
    for (Pair<String, String> pair : plan.getPipeAttributes()) {
      pair.left = pair.left.toLowerCase();
      if ("syncdelop".equals(pair.left)) {
        syncDelOp = Boolean.parseBoolean(pair.right);
      } else {
        throw new PipeException(String.format("Can not recognition attribute %s", pair.left));
      }
    }

    return new TsFilePipeInfo(
        plan.getPipeName(),
        pipeSink.getPipeSinkName(),
        pipeCreateTime,
        plan.getDataStartTimestamp(),
        syncDelOp);
  }

  public static PipeInfo parseCreatePipePlanAsPipeInfo(
      CreatePipeStatement createPipeStatement, PipeSink pipeSink, long pipeCreateTime)
      throws PipeException {
    boolean syncDelOp = true;
    for (Map.Entry<String, String> entry : createPipeStatement.getPipeAttributes().entrySet()) {
      String attributeKey = entry.getKey().toLowerCase();
      if ("syncdelop".equals(attributeKey)) {
        syncDelOp = Boolean.parseBoolean(entry.getValue());
      } else {
        throw new PipeException(String.format("Can not recognition attribute %s", entry.getKey()));
      }
    }

    return new TsFilePipeInfo(
        createPipeStatement.getPipeName(),
        pipeSink.getPipeSinkName(),
        pipeCreateTime,
        createPipeStatement.getStartTime(),
        syncDelOp);
  }

  /** parse PipeInfo ass Pipe, ignore status */
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
}
