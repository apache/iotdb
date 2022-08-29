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
import org.apache.iotdb.confignode.procedure.impl.AddConfigNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.CreateRegionGroupsProcedure;
import org.apache.iotdb.confignode.procedure.impl.DeleteStorageGroupProcedure;
import org.apache.iotdb.confignode.procedure.impl.RegionMigrateProcedure;
import org.apache.iotdb.confignode.procedure.impl.RemoveConfigNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.RemoveDataNodeProcedure;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ProcedureFactory implements IProcedureFactory {

  @Override
  public Procedure create(ByteBuffer buffer) throws IOException {
    int typeNum = buffer.getInt();
    if (typeNum >= ProcedureType.values().length) {
      throw new IOException("unrecognized log type " + typeNum);
    }
    ProcedureType type = ProcedureType.values()[typeNum];
    Procedure procedure;
    switch (type) {
      case DELETE_STORAGE_GROUP_PROCEDURE:
        procedure = new DeleteStorageGroupProcedure();
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
      default:
        throw new IOException("unknown Procedure type: " + typeNum);
    }
    procedure.deserialize(buffer);
    return procedure;
  }

  public static ProcedureType getProcedureType(Procedure procedure) {
    if (procedure instanceof DeleteStorageGroupProcedure) {
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
    }
    return null;
  }

  public enum ProcedureType {
    DELETE_STORAGE_GROUP_PROCEDURE,
    ADD_CONFIG_NODE_PROCEDURE,
    REMOVE_CONFIG_NODE_PROCEDURE,
    REMOVE_DATA_NODE_PROCEDURE,
    REGION_MIGRATE_PROCEDURE,
    CREATE_REGION_GROUPS
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
