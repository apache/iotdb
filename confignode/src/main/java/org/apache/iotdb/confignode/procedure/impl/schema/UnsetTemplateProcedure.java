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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.statemachine.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.UnsetTemplateState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.db.exception.metadata.template.TemplateIsInUseException;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.template.TemplateInternalRPCUtil;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class UnsetTemplateProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, UnsetTemplateState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnsetTemplateProcedure.class);

  private String queryId;
  private Template template;
  private PartialPath path;

  private transient ByteBuffer addTemplateSetInfo;
  private transient ByteBuffer invalidateTemplateSetInfo;

  public UnsetTemplateProcedure() {
    super();
  }

  public UnsetTemplateProcedure(String queryId, Template template, PartialPath path) {
    super();
    this.queryId = queryId;
    this.template = template;
    this.path = path;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, UnsetTemplateState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CONSTRUCT_BLACK_LIST:
          LOGGER.info(
              "Construct schema black list of template {} set on {}", template.getName(), path);
          constructBlackList(env);
          break;
        case CLEAN_DATANODE_TEMPLATE_CACHE:
          LOGGER.info("Invalidate cache of template {} set on {}", template.getName(), path);
          invalidateCache(env);
          break;
        case CHECK_DATANODE_TEMPLATE_ACTIVATION:
          LOGGER.info(
              "Check DataNode template activation of template {} set on {}",
              template.getName(),
              path);
          if (checkDataNodeTemplateActivation(env) > 0) {
            setFailure(new ProcedureException(new TemplateIsInUseException(path.getFullPath())));
          } else {
            setNextState(UnsetTemplateState.UNSET_SCHEMA_TEMPLATE);
          }
          break;
        case UNSET_SCHEMA_TEMPLATE:
          LOGGER.info("Unset template {} on {}", template.getName(), path);
          unsetTemplate(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized state " + state.toString()));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          String.format(
              "UnsetTemplate-[%s] costs %sms",
              state.toString(), (System.currentTimeMillis() - startTime)));
    }
  }

  private void constructBlackList(ConfigNodeProcedureEnv env) {
    TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .preUnsetSchemaTemplate(template.getId(), path);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(UnsetTemplateState.CLEAN_DATANODE_TEMPLATE_CACHE);
    } else {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  private void invalidateCache(ConfigNodeProcedureEnv env) {}

  private int checkDataNodeTemplateActivation(ConfigNodeProcedureEnv env) {
    return 0;
  }

  private void unsetTemplate(ConfigNodeProcedureEnv env) {
    TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .preUnsetSchemaTemplate(template.getId(), path);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(UnsetTemplateState.CLEAN_DATANODE_TEMPLATE_CACHE);
    } else {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, UnsetTemplateState unsetTemplateState)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected boolean isRollbackSupported(UnsetTemplateState unsetTemplateState) {
    return true;
  }

  @Override
  protected UnsetTemplateState getState(int stateId) {
    return UnsetTemplateState.values()[stateId];
  }

  @Override
  protected int getStateId(UnsetTemplateState unsetTemplateState) {
    return unsetTemplateState.ordinal();
  }

  @Override
  protected UnsetTemplateState getInitialState() {
    return UnsetTemplateState.CONSTRUCT_BLACK_LIST;
  }

  public String getQueryId() {
    return queryId;
  }

  public int getTemplateId() {
    return template.getId();
  }

  public String getTemplateName() {
    return template.getName();
  }

  public Template getTemplate() {
    return template;
  }

  public PartialPath getPath() {
    return path;
  }

  private ByteBuffer getAddTemplateSetInfo() {
    if (this.addTemplateSetInfo == null) {
      this.addTemplateSetInfo =
          ByteBuffer.wrap(
              TemplateInternalRPCUtil.generateAddTemplateSetInfoBytes(
                  template, path.getFullPath()));
    }

    return addTemplateSetInfo;
  }

  private ByteBuffer getInvalidateTemplateSetInfo() {
    if (this.invalidateTemplateSetInfo == null) {
      this.invalidateTemplateSetInfo =
          ByteBuffer.wrap(
              TemplateInternalRPCUtil.generateInvalidateTemplateSetInfoBytes(
                  template.getId(), path.getFullPath()));
    }
    return this.invalidateTemplateSetInfo;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeInt(ProcedureFactory.ProcedureType.UNSET_TEMPLATE_PROCEDURE.ordinal());
    super.serialize(stream);
    ReadWriteIOUtils.write(queryId, stream);
    template.serialize(stream);
    path.serialize(stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    queryId = ReadWriteIOUtils.readString(byteBuffer);
    template = new Template();
    template.deserialize(byteBuffer);
    path = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
  }
}
