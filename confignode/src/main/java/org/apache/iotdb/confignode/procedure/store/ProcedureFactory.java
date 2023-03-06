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

package org.apache.iotdb.confignode.procedure.store;

import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.impl.cq.CreateCQProcedure;
import org.apache.iotdb.confignode.procedure.impl.model.CreateModelProcedure;
import org.apache.iotdb.confignode.procedure.impl.model.DropModelProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.AddConfigNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.RemoveConfigNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.RemoveDataNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeactivateTemplateProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteDatabaseProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteTimeSeriesProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.UnsetTemplateProcedure;
import org.apache.iotdb.confignode.procedure.impl.statemachine.CreateRegionGroupsProcedure;
import org.apache.iotdb.confignode.procedure.impl.statemachine.RegionMigrateProcedure;
import org.apache.iotdb.confignode.procedure.impl.sync.CreatePipeProcedure;
import org.apache.iotdb.confignode.procedure.impl.sync.DropPipeProcedure;
import org.apache.iotdb.confignode.procedure.impl.sync.StartPipeProcedure;
import org.apache.iotdb.confignode.procedure.impl.sync.StopPipeProcedure;
import org.apache.iotdb.confignode.procedure.impl.trigger.CreateTriggerProcedure;
import org.apache.iotdb.confignode.procedure.impl.trigger.DropTriggerProcedure;
import org.apache.iotdb.confignode.service.ConfigNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ProcedureFactory implements IProcedureFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureFactory.class);

  @Override
  public Procedure create(ByteBuffer buffer) throws IOException {
    short typeCode = buffer.getShort();
    ProcedureType procedureType = ProcedureType.convertToProcedureType(typeCode);
    if (procedureType == null) {
      LOGGER.error("unrecognized log type " + typeCode);
      throw new IOException("unrecognized log type " + typeCode);
    }

    Procedure procedure;
    switch (procedureType) {
      case DELETE_STORAGE_GROUP_PROCEDURE:
        procedure = new DeleteDatabaseProcedure();
        break;
      case ADD_CONFIG_NODE_PROCEDURE:
        procedure = new AddConfigNodeProcedure();
        break;
      case REMOVE_CONFIG_NODE_PROCEDURE:
        procedure = new RemoveConfigNodeProcedure();
        break;
      case REMOVE_DATA_NODE_PROCEDURE:
        procedure = new RemoveDataNodeProcedure();
        break;
      case REGION_MIGRATE_PROCEDURE:
        procedure = new RegionMigrateProcedure();
        break;
      case CREATE_REGION_GROUPS:
        procedure = new CreateRegionGroupsProcedure();
        break;
      case DELETE_TIMESERIES_PROCEDURE:
        procedure = new DeleteTimeSeriesProcedure();
        break;
      case CREATE_TRIGGER_PROCEDURE:
        procedure = new CreateTriggerProcedure();
        break;
      case DROP_TRIGGER_PROCEDURE:
        procedure = new DropTriggerProcedure();
        break;
      case CREATE_PIPE_PROCEDURE:
        procedure = new CreatePipeProcedure();
        break;
      case START_PIPE_PROCEDURE:
        procedure = new StartPipeProcedure();
        break;
      case STOP_PIPE_PROCEDURE:
        procedure = new StopPipeProcedure();
        break;
      case DROP_PIPE_PROCEDURE:
        procedure = new DropPipeProcedure();
        break;
      case CREATE_CQ_PROCEDURE:
        procedure =
            new CreateCQProcedure(
                ConfigNode.getInstance().getConfigManager().getCQManager().getExecutor());
        break;
      case DEACTIVATE_TEMPLATE_PROCEDURE:
        procedure = new DeactivateTemplateProcedure();
        break;
      case UNSET_TEMPLATE_PROCEDURE:
        procedure = new UnsetTemplateProcedure();
        break;
      case CREATE_MODEL_PROCEDURE:
        procedure = new CreateModelProcedure();
        break;
      case DROP_MODEL_PROCEDURE:
        procedure = new DropModelProcedure();
        break;
      default:
        LOGGER.error("unknown Procedure type: " + typeCode);
        throw new IOException("unknown Procedure type: " + typeCode);
    }
    procedure.deserialize(buffer);
    return procedure;
  }

  public static ProcedureType getProcedureType(Procedure procedure) {
    if (procedure instanceof DeleteDatabaseProcedure) {
      return ProcedureType.DELETE_STORAGE_GROUP_PROCEDURE;
    } else if (procedure instanceof AddConfigNodeProcedure) {
      return ProcedureType.ADD_CONFIG_NODE_PROCEDURE;
    } else if (procedure instanceof RemoveConfigNodeProcedure) {
      return ProcedureType.REMOVE_CONFIG_NODE_PROCEDURE;
    } else if (procedure instanceof RemoveDataNodeProcedure) {
      return ProcedureType.REMOVE_DATA_NODE_PROCEDURE;
    } else if (procedure instanceof RegionMigrateProcedure) {
      return ProcedureType.REGION_MIGRATE_PROCEDURE;
    } else if (procedure instanceof CreateRegionGroupsProcedure) {
      return ProcedureType.CREATE_REGION_GROUPS;
    } else if (procedure instanceof DeleteTimeSeriesProcedure) {
      return ProcedureType.DELETE_TIMESERIES_PROCEDURE;
    } else if (procedure instanceof CreateTriggerProcedure) {
      return ProcedureType.CREATE_TRIGGER_PROCEDURE;
    } else if (procedure instanceof DropTriggerProcedure) {
      return ProcedureType.DROP_TRIGGER_PROCEDURE;
    } else if (procedure instanceof CreatePipeProcedure) {
      return ProcedureType.CREATE_PIPE_PROCEDURE;
    } else if (procedure instanceof StartPipeProcedure) {
      return ProcedureType.START_PIPE_PROCEDURE;
    } else if (procedure instanceof StopPipeProcedure) {
      return ProcedureType.STOP_PIPE_PROCEDURE;
    } else if (procedure instanceof DropPipeProcedure) {
      return ProcedureType.DROP_PIPE_PROCEDURE;
    } else if (procedure instanceof CreateCQProcedure) {
      return ProcedureType.CREATE_CQ_PROCEDURE;
    } else if (procedure instanceof DeactivateTemplateProcedure) {
      return ProcedureType.DEACTIVATE_TEMPLATE_PROCEDURE;
    } else if (procedure instanceof UnsetTemplateProcedure) {
      return ProcedureType.UNSET_TEMPLATE_PROCEDURE;
    }
    return null;
  }

  private static class ProcedureFactoryHolder {

    private static final ProcedureFactory INSTANCE = new ProcedureFactory();

    private ProcedureFactoryHolder() {
      // Empty constructor
    }
  }

  public static ProcedureFactory getInstance() {
    return ProcedureFactoryHolder.INSTANCE;
  }
}
