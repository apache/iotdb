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

package org.apache.iotdb.postgresql.catalog;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.common.DataNodeEndPoints;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.InformationSchemaContentSupplierFactory.IInformationSchemaContentSupplier;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.InformationSchemaContentSupplierFactory.TsBlockSupplier;
import org.apache.iotdb.db.queryengine.plan.execution.config.TableConfigTaskVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.information.AdditionalInformationSchemaProvider;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.InformationSchemaTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BytesUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Supplies the rows of the catalog tables per query, for the querying user. The tables are
 * evaluated on the local DataNode: the content comes from its table cache.
 */
public final class PgCatalogProvider implements AdditionalInformationSchemaProvider {

  @Override
  public IInformationSchemaContentSupplier getContentSupplier(
      String tableName,
      OperatorContext context,
      List<TSDataType> dataTypes,
      UserEntity userEntity,
      InformationSchemaTableScanNode node) {
    if (!PgCatalogTables.contains(tableName)) {
      return null;
    }
    List<String> columns = new ArrayList<>();
    for (Symbol symbol : node.getOutputSymbols()) {
      columns.add(node.getAssignments().get(symbol).getName());
    }
    CatalogRows catalog = new CatalogRows(userEntity.getUsername(), () -> snapshot(userEntity));
    return new RowSupplier(dataTypes, columns, catalog.rows(tableName));
  }

  @Override
  public List<TDataNodeLocation> getTableLocation(String tableName) {
    return PgCatalogTables.contains(tableName)
        ? Collections.singletonList(DataNodeEndPoints.getLocalDataNodeLocation())
        : null;
  }

  private static CatalogSnapshot snapshot(UserEntity user) {
    AccessControl access = AuthorityChecker.getAccessControl();
    String name = user.getUsername();
    return CatalogSnapshot.of(
        DataNodeTableCache.getInstance().getTableSnapshot(),
        database -> TableConfigTaskVisitor.canShowDB(access, name, database, user),
        (database, table) ->
            TableConfigTaskVisitor.canShowTable(access, name, database, table, user));
  }

  /** Writes rows into blocks, one column per output symbol of the scan. */
  private static final class RowSupplier extends TsBlockSupplier {
    private final List<TSDataType> types;
    private final List<String> columns;
    private final List<Map<String, Object>> rows;
    private int next;

    RowSupplier(List<TSDataType> types, List<String> columns, List<Map<String, Object>> rows) {
      super(types);
      this.types = types;
      this.columns = columns;
      this.rows = rows;
    }

    @Override
    public boolean hasNext() {
      return next < rows.size();
    }

    @Override
    protected void constructLine() {
      Map<String, Object> row = rows.get(next++);
      for (int i = 0; i < columns.size(); i++) {
        TSDataType type = types.get(i);
        Object value = CatalogRows.convert(row.get(columns.get(i)), type);
        if (value == null) {
          columnBuilders[i].appendNull();
          continue;
        }
        switch (type) {
          case INT64:
            columnBuilders[i].writeLong((Long) value);
            break;
          case INT32:
            columnBuilders[i].writeInt((Integer) value);
            break;
          case FLOAT:
            columnBuilders[i].writeFloat((Float) value);
            break;
          case DOUBLE:
            columnBuilders[i].writeDouble((Double) value);
            break;
          case BOOLEAN:
            columnBuilders[i].writeBoolean((Boolean) value);
            break;
          default:
            columnBuilders[i].writeBinary(BytesUtils.valueOf((String) value));
        }
      }
      resultBuilder.declarePosition();
    }
  }
}
