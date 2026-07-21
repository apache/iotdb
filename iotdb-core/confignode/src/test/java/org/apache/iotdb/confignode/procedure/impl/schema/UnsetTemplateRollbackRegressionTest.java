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
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.schema.UnsetTemplateState;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

public class UnsetTemplateRollbackRegressionTest {

  @Test
  public void rollbackMustRestoreConfigNodeStateWhenDataNodeCacheRepairFails() throws Exception {
    final Template template = new Template();
    template.setId(1);
    template.setName("t1");
    final PartialPath path = new PartialPath("root.sg");
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ClusterSchemaManager schemaManager = Mockito.mock(ClusterSchemaManager.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getClusterSchemaManager()).thenReturn(schemaManager);

    final FailingDataNodeRollbackProcedure procedure =
        new FailingDataNodeRollbackProcedure(template, path);
    procedure.rollbackAfterActivationCheck(env);

    Mockito.verify(schemaManager).rollbackPreUnsetSchemaTemplate(template.getId(), path);
  }

  private static class FailingDataNodeRollbackProcedure extends UnsetTemplateProcedure {

    private FailingDataNodeRollbackProcedure(final Template template, final PartialPath path) {
      super("test", template, path, false);
    }

    @Override
    void executeRollbackInvalidateCache(final ConfigNodeProcedureEnv env)
        throws ProcedureException {
      throw new ProcedureException("injected offline DataNode");
    }

    @Override
    void executeInvalidateCache(final ConfigNodeProcedureEnv env) {
      // No-op: this test isolates ordering in the rollback path.
    }

    private void rollbackAfterActivationCheck(final ConfigNodeProcedureEnv env)
        throws IOException, InterruptedException, ProcedureException {
      rollbackState(env, UnsetTemplateState.CHECK_DATANODE_TEMPLATE_ACTIVATION);
    }
  }
}
