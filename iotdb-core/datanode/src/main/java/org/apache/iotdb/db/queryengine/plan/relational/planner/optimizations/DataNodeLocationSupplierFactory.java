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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.confignode.rpc.thrift.TGetDataNodeLocationsResp;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;

import java.util.List;

import static org.apache.iotdb.commons.schema.table.InformationSchemaTable.QUERIES;
import static org.apache.iotdb.rpc.TSStatusCode.QUERY_PROCESS_ERROR;

public class DataNodeLocationSupplierFactory {
  private DataNodeLocationSupplierFactory() {}

  public static DataNodeLocationSupplier getSupplier() {
    return InformationSchemaTableDataNodeLocationSupplier.getInstance();
  }

  public interface DataNodeLocationSupplier {
    List<TDataNodeLocation> getDataNodeLocations(String table);
  }

  private static class InformationSchemaTableDataNodeLocationSupplier
      implements DataNodeLocationSupplier {
    private InformationSchemaTableDataNodeLocationSupplier() {}

    private static class SingletonHolder {
      private static final InformationSchemaTableDataNodeLocationSupplier INSTANCE =
          new InformationSchemaTableDataNodeLocationSupplier();
    }

    private static InformationSchemaTableDataNodeLocationSupplier getInstance() {
      return SingletonHolder.INSTANCE;
    }

    private List<TDataNodeLocation> getRunningDataNodeLocations() {
      try (ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        TGetDataNodeLocationsResp showDataNodesResp = client.getRunningDataNodeLocations();
        if (showDataNodesResp.getStatus().getCode()
            != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new IoTDBRuntimeException(
              "An error occurred when executing getRunningDataNodeLocations():"
                  + showDataNodesResp.getStatus().getMessage(),
              QUERY_PROCESS_ERROR.getStatusCode());
        }
        return showDataNodesResp.getDataNodeLocationList();
      } catch (ClientManagerException | TException e) {
        throw new IoTDBRuntimeException(
            "An error occurred when executing getRunningDataNodeLocations():" + e.getMessage(),
            QUERY_PROCESS_ERROR.getStatusCode());
      }
    }

    @Override
    public List<TDataNodeLocation> getDataNodeLocations(String tableName) {
      if (tableName.equals(QUERIES.getSchemaTableName())) {
        return getRunningDataNodeLocations();
      } else {
        throw new UnsupportedOperationException("Unknown table: " + tableName);
      }
    }
  }
}
