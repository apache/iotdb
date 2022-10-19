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
import org.apache.iotdb.confignode.procedure.state.schema.DeactivateTemplateState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class DeactivateTemplateProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, DeactivateTemplateState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeactivateTemplateProcedure.class);

  private String queryId;
  private Map<PartialPath, List<Template>> templateSetInfo;

  private String requestMessage;

  public DeactivateTemplateProcedure() {
    super();
  }

  public DeactivateTemplateProcedure(
      String queryId, Map<PartialPath, List<Template>> templateSetInfo) {
    super();
    this.queryId = queryId;
    setTemplateSetInfo(templateSetInfo);
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, DeactivateTemplateState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CONSTRUCT_BLACK_LIST:
          LOGGER.info("Construct schema black list of timeseries {}", requestMessage);
          if (constructBlackList(env) > 0) {
            setNextState(DeactivateTemplateState.CLEAN_DATANODE_SCHEMA_CACHE);
            break;
          } else {
            return Flow.NO_MORE_STATE;
          }
        case CLEAN_DATANODE_SCHEMA_CACHE:
          LOGGER.info("Invalidate cache of timeseries {}", requestMessage);
          invalidateCache(env);
          break;
        case DELETE_DATA:
          LOGGER.info("Delete data of timeseries {}", requestMessage);
          deleteData(env);
          break;
        case DEACTIVATE_TEMPLATE:
          LOGGER.info("Delete timeseries schema of {}", requestMessage);
          deactivateTemplate(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized state " + state.toString()));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          String.format(
              "DeleteTimeSeries-[%s] costs %sms",
              state.toString(), (System.currentTimeMillis() - startTime)));
    }
  }

  // return the total num of timeseries in schema black list
  private int constructBlackList(ConfigNodeProcedureEnv env) {
    return 0;
  }

  private void invalidateCache(ConfigNodeProcedureEnv env) {}

  private void deleteData(ConfigNodeProcedureEnv env) {}

  private void deactivateTemplate(ConfigNodeProcedureEnv env) {}

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv env, DeactivateTemplateState deactivateTemplateState)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected boolean isRollbackSupported(DeactivateTemplateState deactivateTemplateState) {
    return true;
  }

  @Override
  protected DeactivateTemplateState getState(int stateId) {
    return DeactivateTemplateState.values()[stateId];
  }

  @Override
  protected int getStateId(DeactivateTemplateState deactivateTemplateState) {
    return deactivateTemplateState.ordinal();
  }

  @Override
  protected DeactivateTemplateState getInitialState() {
    return DeactivateTemplateState.CONSTRUCT_BLACK_LIST;
  }

  public String getQueryId() {
    return queryId;
  }

  public Map<PartialPath, List<Template>> getTemplateSetInfo() {
    return templateSetInfo;
  }

  private void setTemplateSetInfo(Map<PartialPath, List<Template>> templateSetInfo) {
    this.templateSetInfo = templateSetInfo;
    prepareRequestMessage(templateSetInfo);
  }

  private void prepareRequestMessage(Map<PartialPath, List<Template>> templateSetInfo) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("{");
    for (Map.Entry<PartialPath, List<Template>> entry : templateSetInfo.entrySet()) {
      stringBuilder
          .append(entry.getKey())
          .append(":")
          .append(entry.getValue().stream().map(Template::getName).collect(Collectors.toList()))
          .append(";");
    }
    stringBuilder.append("}");
    this.requestMessage = stringBuilder.toString();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeInt(ProcedureFactory.ProcedureType.DEACTIVATE_TEMPLATE_PROCEDURE.ordinal());
    super.serialize(stream);
    ReadWriteIOUtils.write(queryId, stream);
    ReadWriteIOUtils.write(templateSetInfo.size(), stream);
    for (Map.Entry<PartialPath, List<Template>> entry : templateSetInfo.entrySet()) {
      entry.getKey().serialize(stream);
      ReadWriteIOUtils.write(entry.getValue().size(), stream);
      for (Template template : entry.getValue()) {
        template.serialize(stream);
      }
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    queryId = ReadWriteIOUtils.readString(byteBuffer);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    Map<PartialPath, List<Template>> templateSetInfo = new HashMap<>();
    for (int i = 0; i < size; i++) {
      PartialPath pattern = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
      int templateNum = ReadWriteIOUtils.readInt(byteBuffer);
      List<Template> templateList = new ArrayList<>(templateNum);
      for (int j = 0; j < templateNum; j++) {
        Template template = new Template();
        template.deserialize(byteBuffer);
        templateList.add(template);
      }
      templateSetInfo.put(pattern, templateList);
    }
    setTemplateSetInfo(templateSetInfo);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DeactivateTemplateProcedure that = (DeactivateTemplateProcedure) o;
    return Objects.equals(queryId, that.queryId)
        && Objects.equals(templateSetInfo, that.templateSetInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(queryId, templateSetInfo);
  }
}
