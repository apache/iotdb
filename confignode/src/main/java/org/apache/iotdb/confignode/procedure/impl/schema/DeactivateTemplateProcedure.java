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
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.statemachine.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.DeactivateTemplateState;
import org.apache.iotdb.db.metadata.template.Template;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DeactivateTemplateProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, DeactivateTemplateState> {

  private String queryId;
  private Map<PartialPath, List<Template>> templateSetInfo;

  public DeactivateTemplateProcedure() {}

  public DeactivateTemplateProcedure(
      String queryId, Map<PartialPath, List<Template>> templateSetInfo) {}

  @Override
  protected Flow executeFromState(
      ConfigNodeProcedureEnv env, DeactivateTemplateState deactivateTemplateState)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    return null;
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv env, DeactivateTemplateState deactivateTemplateState)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected DeactivateTemplateState getState(int stateId) {
    return null;
  }

  @Override
  protected int getStateId(DeactivateTemplateState deactivateTemplateState) {
    return 0;
  }

  @Override
  protected DeactivateTemplateState getInitialState() {
    return null;
  }

  public String getQueryId() {
    return queryId;
  }

  public Map<PartialPath, List<Template>> getTemplateSetInfo() {
    return templateSetInfo;
  }
}
