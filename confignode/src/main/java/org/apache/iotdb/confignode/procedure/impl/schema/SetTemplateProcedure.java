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
import org.apache.iotdb.confignode.consensus.request.read.template.CheckTemplateSettablePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.PreSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.response.template.TemplateInfoResp;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.statemachine.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.SetTemplateState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class SetTemplateProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, SetTemplateState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SetTemplateProcedure.class);

  private String templateName;
  private String templateSetPath;

  public SetTemplateProcedure() {
    super();
  }

  public SetTemplateProcedure(String templateName, String templateSetPath) {
    super();
    this.templateName = templateName;
    this.templateSetPath = templateSetPath;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, SetTemplateState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case VALIDATE_TEMPLATE_EXISTENCE:
          LOGGER.info(
              "Check template existence set on path {} when try setting template {}",
              templateSetPath,
              templateName);
          validateTemplateExistence(env);
          break;
        case PRE_SET:
          LOGGER.info("Pre set schema template {} on path {}", templateName, templateSetPath);
          preSetTemplate(env);
          break;
        case PRE_RELEASE:
          LOGGER.info(
              "Pre release schema template {} set on path {}", templateName, templateSetPath);
          preReleaseTemplate(env);
          setNextState(SetTemplateState.VALIDATE_TIMESERIES_EXISTENCE);
          break;
        case VALIDATE_TIMESERIES_EXISTENCE:
          LOGGER.info(
              "Check timeseries existence under path {} when try setting template {}",
              templateSetPath,
              templateName);
          validateTimeSeriesExistence(env);
          setNextState(SetTemplateState.COMMIT_SET);
          break;
        case COMMIT_SET:
          LOGGER.info("Commit set schema template {} on path {}", templateName, templateSetPath);
          commitSetTemplate(env);
          setNextState(SetTemplateState.COMMIT_RELEASE);
          break;
        case COMMIT_RELEASE:
          LOGGER.info(
              "Commit release schema template {} set on path {}", templateName, templateSetPath);
          commitReleaseTemplate(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized SetTemplateState " + state.toString()));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          String.format(
              "SetSchemaTemplate-[%s] costs %sms",
              state.toString(), (System.currentTimeMillis() - startTime)));
    }
  }

  private void validateTemplateExistence(ConfigNodeProcedureEnv env) {
    // check whether the template can be set on given path
    CheckTemplateSettablePlan checkTemplateSettablePlan =
        new CheckTemplateSettablePlan(templateName, templateSetPath);
    TemplateInfoResp resp =
        (TemplateInfoResp)
            env.getConfigManager()
                .getConsensusManager()
                .read(checkTemplateSettablePlan)
                .getDataset();
    if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(SetTemplateState.PRE_SET);
    } else {
      setFailure(
          new ProcedureException(
              new IoTDBException(resp.getStatus().getMessage(), resp.getStatus().getCode())));
    }
  }

  private void preSetTemplate(ConfigNodeProcedureEnv env) {
    PreSetSchemaTemplatePlan preSetSchemaTemplatePlan =
        new PreSetSchemaTemplatePlan(templateName, templateSetPath);
    TSStatus status =
        env.getConfigManager().getConsensusManager().write(preSetSchemaTemplatePlan).getStatus();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(SetTemplateState.PRE_RELEASE);
    } else {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  private void preReleaseTemplate(ConfigNodeProcedureEnv env) {}

  private void validateTimeSeriesExistence(ConfigNodeProcedureEnv env) {}

  private void commitSetTemplate(ConfigNodeProcedureEnv env) {}

  private void commitReleaseTemplate(ConfigNodeProcedureEnv env) {}

  @Override
  protected boolean isRollbackSupported(SetTemplateState setTemplateState) {
    return true;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, SetTemplateState state)
      throws IOException, InterruptedException, ProcedureException {
    switch (state) {
      case PRE_SET:
        rollbackPreSet(env);
        break;
      case PRE_RELEASE:
        rollbackPreRelease(env);
        break;
      case COMMIT_SET:
        rollbackCommitSet(env);
        break;
    }
  }

  private void rollbackPreSet(ConfigNodeProcedureEnv env) {}

  private void rollbackPreRelease(ConfigNodeProcedureEnv env) {}

  private void rollbackCommitSet(ConfigNodeProcedureEnv env) {}

  @Override
  protected SetTemplateState getState(int stateId) {
    return SetTemplateState.values()[stateId];
  }

  @Override
  protected int getStateId(SetTemplateState state) {
    return state.ordinal();
  }

  @Override
  protected SetTemplateState getInitialState() {
    return SetTemplateState.PRE_SET;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.SET_TEMPLATE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(templateName, stream);
    ReadWriteIOUtils.write(templateSetPath, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    templateName = ReadWriteIOUtils.readString(byteBuffer);
    templateSetPath = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SetTemplateProcedure that = (SetTemplateProcedure) o;
    return Objects.equals(templateName, that.templateName)
        && Objects.equals(templateSetPath, that.templateSetPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(templateName, templateSetPath);
  }
}
