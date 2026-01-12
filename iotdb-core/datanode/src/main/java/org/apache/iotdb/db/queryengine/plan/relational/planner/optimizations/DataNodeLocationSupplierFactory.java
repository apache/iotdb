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
import org.apache.iotdb.commons.schema.table.InformationSchema;
import org.apache.iotdb.confignode.rpc.thrift.TGetDataNodeLocationsResp;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.common.DataNodeEndPoints;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;

import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.rpc.TSStatusCode.QUERY_PROCESS_ERROR;

public class DataNodeLocationSupplierFactory {

  private DataNodeLocationSupplierFactory() {}

  public static DataNodeLocationSupplier getSupplier() {
    return InformationSchemaTableDataNodeLocationSupplier.getInstance();
  }

  public interface DataNodeLocationSupplier {
    List<TDataNodeLocation> getDataNodeLocations(String table);
  }

  /** DataNode in these states is readable: Running, ReadOnly, Removing */
  public static List<TDataNodeLocation> getReadableDataNodeLocations() {
    try (final ConfigNodeClient client =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TGetDataNodeLocationsResp showDataNodesResp = client.getReadableDataNodeLocations();
      if (showDataNodesResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new IoTDBRuntimeException(
            "An error occurred when executing getReadableDataNodeLocations():"
                + showDataNodesResp.getStatus().getMessage(),
            QUERY_PROCESS_ERROR.getStatusCode());
      }
      return showDataNodesResp.getDataNodeLocationList();
    } catch (final ClientManagerException | TException e) {
      throw new IoTDBRuntimeException(
          "An error occurred when executing getReadableDataNodeLocations():" + e.getMessage(),
          QUERY_PROCESS_ERROR.getStatusCode());
    }
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

    @Override
    public List<TDataNodeLocation> getDataNodeLocations(final String tableName) {
      switch (tableName) {
        case InformationSchema.QUERIES:
        case InformationSchema.CONNECTIONS:
        case InformationSchema.CURRENT_QUERIES:
        case InformationSchema.QUERIES_COSTS_HISTOGRAM:
          return getReadableDataNodeLocations();
        case InformationSchema.DATABASES:
        case InformationSchema.TABLES:
        case InformationSchema.COLUMNS:
        case InformationSchema.REGIONS:
        case InformationSchema.PIPES:
        case InformationSchema.PIPE_PLUGINS:
        case InformationSchema.TOPICS:
        case InformationSchema.SUBSCRIPTIONS:
        case InformationSchema.VIEWS:
        case InformationSchema.FUNCTIONS:
        case InformationSchema.CONFIGURATIONS:
        case InformationSchema.KEYWORDS:
        case InformationSchema.NODES:
        case InformationSchema.CONFIG_NODES:
        case InformationSchema.DATA_NODES:
        case InformationSchema.SERVICES:
          return Collections.singletonList(DataNodeEndPoints.getLocalDataNodeLocation());
        default:
          throw new UnsupportedOperationException("Unknown table: " + tableName);
      }
    }
  }
}
