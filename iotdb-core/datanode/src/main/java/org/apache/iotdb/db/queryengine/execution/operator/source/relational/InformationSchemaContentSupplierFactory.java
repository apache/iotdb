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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.schema.table.InformationSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseInfo;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;

import java.security.AccessControlException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_SCOPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_RESULT_NODES;

public class InformationSchemaContentSupplierFactory {
  private InformationSchemaContentSupplierFactory() {}

  public static Iterator<TsBlock> getSupplier(
      final String tableName, final List<TSDataType> dataTypes, final String userName) {
    if (tableName.equals(InformationSchema.QUERIES)) {
      return new QueriesSupplier(dataTypes);
    } else if (tableName.equals(InformationSchema.DATABASES)) {
      return new DatabaseSupplier(dataTypes, userName);
    } else {
      throw new UnsupportedOperationException("Unknown table: " + tableName);
    }
  }

  private static class QueriesSupplier extends TsBlockSupplier {
    private final long currTime = System.currentTimeMillis();
    private final List<IQueryExecution> queryExecutions =
        Coordinator.getInstance().getAllQueryExecutions();

    private QueriesSupplier(final List<TSDataType> dataTypes) {
      super(dataTypes);
      this.totalSize = queryExecutions.size();
    }

    @Override
    protected void constructLine() {
      IQueryExecution queryExecution = queryExecutions.get(nextConsumedIndex);

      if (queryExecution.getSQLDialect().equals(IClientSession.SqlDialect.TABLE)) {
        String[] splits = queryExecution.getQueryId().split("_");
        int dataNodeId = Integer.parseInt(splits[splits.length - 1]);

        columnBuilders[0].writeBinary(BytesUtils.valueOf(queryExecution.getQueryId()));
        columnBuilders[1].writeLong(queryExecution.getStartExecutionTime());
        columnBuilders[2].writeInt(dataNodeId);
        columnBuilders[3].writeFloat(
            (float) (currTime - queryExecution.getStartExecutionTime()) / 1000);
        columnBuilders[4].writeBinary(
            BytesUtils.valueOf(queryExecution.getExecuteSQL().orElse("UNKNOWN")));
        resultBuilder.declarePosition();
      }
    }
  }

  private static class DatabaseSupplier extends TsBlockSupplier {
    private Iterator<Map.Entry<String, TDatabaseInfo>> iterator;
    private final String userName;

    private DatabaseSupplier(final List<TSDataType> dataTypes, final String userName) {
      super(dataTypes);
      this.userName = userName;
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        final TShowDatabaseResp resp =
            client.showDatabase(
                new TGetDatabaseReq(Arrays.asList(ALL_RESULT_NODES), ALL_MATCH_SCOPE.serialize())
                    .setIsTableModel(true));
        totalSize = resp.getDatabaseInfoMapSize();
        iterator = resp.getDatabaseInfoMap().entrySet().iterator();
      } catch (final Exception e) {
        // TODO
      }
    }

    @Override
    protected void constructLine() {
      if (!iterator.hasNext()) {
        throw new NoSuchElementException();
      }
      final Map.Entry<String, TDatabaseInfo> info = iterator.next();
      try {
        Coordinator.getInstance()
            .getAccessControl()
            .checkCanShowOrUseDatabase(userName, info.getKey());
      } catch (final AccessControlException e) {
        return;
      }
      final TDatabaseInfo storageGroupInfo = info.getValue();
      columnBuilders[0].writeBinary(new Binary(info.getKey(), TSFileConfig.STRING_CHARSET));

      if (Long.MAX_VALUE == storageGroupInfo.getTTL()) {
        columnBuilders[1].writeBinary(
            new Binary(IoTDBConstant.TTL_INFINITE, TSFileConfig.STRING_CHARSET));
      } else {
        columnBuilders[1].writeBinary(
            new Binary(String.valueOf(storageGroupInfo.getTTL()), TSFileConfig.STRING_CHARSET));
      }
      columnBuilders[2].writeInt(storageGroupInfo.getSchemaReplicationFactor());
      columnBuilders[3].writeInt(storageGroupInfo.getDataReplicationFactor());
      columnBuilders[4].writeLong(storageGroupInfo.getTimePartitionInterval());
      columnBuilders[5].writeInt(storageGroupInfo.getSchemaRegionNum());
      columnBuilders[6].writeInt(storageGroupInfo.getDataRegionNum());
      resultBuilder.declarePosition();
    }
  }

  private abstract static class TsBlockSupplier implements Iterator<TsBlock> {

    protected final TsBlockBuilder resultBuilder;
    protected final ColumnBuilder[] columnBuilders;

    // We initialize it later for the convenience of data preparation
    protected int totalSize;
    protected int nextConsumedIndex;

    private TsBlockSupplier(final List<TSDataType> dataTypes) {
      this.resultBuilder = new TsBlockBuilder(dataTypes);
      this.columnBuilders = resultBuilder.getValueColumnBuilders();
    }

    @Override
    public boolean hasNext() {
      return nextConsumedIndex < totalSize;
    }

    @Override
    public TsBlock next() {
      if (nextConsumedIndex >= totalSize) {
        throw new NoSuchElementException();
      }
      while (nextConsumedIndex < totalSize && !resultBuilder.isFull()) {
        constructLine();
        nextConsumedIndex++;
      }
      TsBlock result =
          resultBuilder.build(
              new RunLengthEncodedColumn(
                  TableScanOperator.TIME_COLUMN_TEMPLATE, resultBuilder.getPositionCount()));
      resultBuilder.reset();
      return result;
    }

    protected abstract void constructLine();
  }
}
