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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class UnsetTemplateProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, UnsetTemplateState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnsetTemplateProcedure.class);

  private String queryId;
  private int templateId;
  private String templateName;
  private PartialPath path;

  public UnsetTemplateProcedure() {
    super();
  }

  public UnsetTemplateProcedure(
      String queryId, int templateId, String templateName, PartialPath path) {
    super();
    this.queryId = queryId;
    this.templateId = templateId;
    this.templateName = templateName;
    this.path = path;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, UnsetTemplateState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CONSTRUCT_BLACK_LIST:
          LOGGER.info("Construct schema black list of template {} set on {}", templateName, path);
          constructBlackList(env);
          break;
        case CLEAN_DATANODE_TEMPLATE_CACHE:
          LOGGER.info("Invalidate cache of template {} set on {}", templateName, path);
          invalidateCache(env);
          break;
        case CHECK_DATANODE_TEMPLATE_ACTIVATION:
          LOGGER.info(
              "Check DataNode template activation of template {} set on {}", templateName, path);
          if (checkDataNodeTemplateActivation(env) > 0) {
            setFailure(new ProcedureException(new TemplateIsInUseException(path.getFullPath())));
          } else {
            setNextState(UnsetTemplateState.UNSET_SCHEMA_TEMPLATE);
          }
          break;
        case UNSET_SCHEMA_TEMPLATE:
          LOGGER.info("Unset template {} on {}", templateName, path);
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

  private void constructBlackList(ConfigNodeProcedureEnv env) {}

  private void invalidateCache(ConfigNodeProcedureEnv env) {}

  private int checkDataNodeTemplateActivation(ConfigNodeProcedureEnv env) {
    return 0;
  }

  private void unsetTemplate(ConfigNodeProcedureEnv env) {}

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
    return templateId;
  }

  public String getTemplateName() {
    return templateName;
  }

  public PartialPath getPath() {
    return path;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeInt(ProcedureFactory.ProcedureType.UNSET_TEMPLATE_PROCEDURE.ordinal());
    super.serialize(stream);
    ReadWriteIOUtils.write(queryId, stream);
    ReadWriteIOUtils.write(templateId, stream);
    ReadWriteIOUtils.write(templateName, stream);
    path.serialize(stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    queryId = ReadWriteIOUtils.readString(byteBuffer);
    templateId = ReadWriteIOUtils.readInt(byteBuffer);
    templateName = ReadWriteIOUtils.readString(byteBuffer);
    path = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UnsetTemplateProcedure that = (UnsetTemplateProcedure) o;
    return templateId == that.templateId
        && Objects.equals(queryId, that.queryId)
        && Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(queryId, templateId, path);
  }
}
