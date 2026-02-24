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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.MetadataUtil;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableDeviceSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExtractCommonPredicatesExpressionRewriter;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AbstractQueryDeviceWithCache.getDeviceColumnHeaderList;

// TODO table metadata: reuse query distinct logic
// Show, Count, Update, Delete Devices
public abstract class AbstractTraverseDevice extends Statement {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AbstractTraverseDevice.class);

  protected String database;

  protected String tableName;
  // Temporary
  protected Table table;

  protected Expression where;

  /**
   * The outer list represents the OR relation between different expression lists.
   *
   * <p>The inner list represents the AND between different expression.
   *
   * <p>Each inner list represents a device pattern and each expression of it represents one
   * condition on some id column.
   */
  protected List<List<SchemaFilter>> tagDeterminedFilterList;

  /** filters/conditions involving non-id columns and concat by OR to id column filters */
  protected Expression tagFuzzyPredicate;

  private List<IDeviceID> partitionKeyList;

  // The "CountDevice"'s column header list is the same as the device's header
  // to help reuse filter operator
  protected List<ColumnHeader> columnHeaderList;

  // If there are no attribute columns, we can skip returning it to save time
  private List<String> attributeColumns;

  // For sql-input show device usage
  protected AbstractTraverseDevice(
      final NodeLocation location, final Table table, final Expression where) {
    super(location);
    this.table = table;
    this.where = where;
  }

  protected AbstractTraverseDevice(final String database, final String tableName) {
    super(null);
    this.database = database;
    this.tableName = tableName;
  }

  public void parseTable(final SessionInfo sessionInfo) {
    if (Objects.isNull(table)) {
      return;
    }
    final QualifiedObjectName objectName =
        MetadataUtil.createQualifiedObjectName(sessionInfo, table.getName());
    database = objectName.getDatabaseName();
    tableName = objectName.getObjectName();
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(final String database) {
    this.database = database;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(final String tableName) {
    this.tableName = tableName;
  }

  public Table getTable() {
    return table;
  }

  public Optional<Expression> getWhere() {
    return Optional.ofNullable(where);
  }

  public void setWhere(final Expression where) {
    this.where = where;
  }

  public boolean parseWhere(
      final Map<String, List<DeviceEntry>> entries,
      final TsTable tableInstance,
      final List<String> attributeColumns,
      final MPPQueryContext context) {
    if (Objects.isNull(where)) {
      return true;
    }
    where = ExtractCommonPredicatesExpressionRewriter.extractCommonPredicates(where);
    return TableDeviceSchemaFetcher.getInstance()
        .parseFilter4TraverseDevice(
            tableInstance,
            (where instanceof LogicalExpression
                    && ((LogicalExpression) where).getOperator() == LogicalExpression.Operator.AND)
                ? ((LogicalExpression) where).getTerms()
                : Collections.singletonList(where),
            this,
            entries,
            attributeColumns,
            context,
            new AtomicBoolean(false),
            true);
  }

  public List<List<SchemaFilter>> getTagDeterminedFilterList() {
    if (tagDeterminedFilterList == null) {
      tagDeterminedFilterList = Collections.singletonList(Collections.emptyList());
    }
    return tagDeterminedFilterList;
  }

  public void setTagDeterminedFilterList(final List<List<SchemaFilter>> tagDeterminedFilterList) {
    this.tagDeterminedFilterList = tagDeterminedFilterList;
  }

  public Expression getTagFuzzyPredicate() {
    return tagFuzzyPredicate;
  }

  public void setTagFuzzyPredicate(final Expression tagFuzzyPredicate) {
    this.tagFuzzyPredicate = tagFuzzyPredicate;
  }

  public boolean isIdDetermined() {
    return Objects.nonNull(partitionKeyList);
  }

  public List<IDeviceID> getPartitionKeyList() {
    return partitionKeyList;
  }

  public void setPartitionKeyList(final List<IDeviceID> partitionKeyList) {
    this.partitionKeyList = partitionKeyList;
  }

  public void setAttributeColumns(final List<String> attributeColumns) {
    this.attributeColumns = attributeColumns;
  }

  public List<String> getAttributeColumns() {
    return attributeColumns;
  }

  public List<ColumnHeader> getColumnHeaderList() {
    return columnHeaderList;
  }

  public void setColumnHeaderList() {
    this.columnHeaderList = getDeviceColumnHeaderList(database, tableName, attributeColumns);
  }

  @Override
  public List<? extends Node> getChildren() {
    final ImmutableList.Builder<Node> nodes = ImmutableList.builder();
    if (where != null) {
      nodes.add(where);
    }
    return nodes.build();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final AbstractTraverseDevice that = (AbstractTraverseDevice) o;
    return Objects.equals(database, that.database)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(where, that.where)
        && Objects.equals(tagDeterminedFilterList, that.tagDeterminedFilterList)
        && Objects.equals(tagFuzzyPredicate, that.tagFuzzyPredicate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, tableName, where, tagDeterminedFilterList, tagFuzzyPredicate);
  }

  protected String toStringContent() {
    return "{"
        + "database='"
        + database
        + '\''
        + ", tableName='"
        + tableName
        + '\''
        + ", rawExpression="
        + where
        + ", idDeterminedFilterList="
        + tagDeterminedFilterList
        + ", idFuzzyFilter="
        + tagFuzzyPredicate
        + '}';
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE + ramBytesUsedForCommonFields();
  }

  protected long ramBytesUsedForCommonFields() {
    long size = 0;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(table);
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(where);
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(tagFuzzyPredicate);
    size += RamUsageEstimator.sizeOf(database);
    size += RamUsageEstimator.sizeOf(tableName);
    if (tagDeterminedFilterList != null) {
      size += RamUsageEstimator.shallowSizeOf(tagDeterminedFilterList);
      for (List<SchemaFilter> filters : tagDeterminedFilterList) {
        if (filters != null) {
          size += RamUsageEstimator.shallowSizeOf(filters);
          for (SchemaFilter filter : filters) {
            size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(filter);
          }
        }
      }
    }
    if (columnHeaderList != null) {
      size += RamUsageEstimator.shallowSizeOf(columnHeaderList);
      for (ColumnHeader header : columnHeaderList) {
        size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(header);
      }
    }
    size += AstMemoryEstimationHelper.getEstimatedSizeOfStringList(attributeColumns);
    if (partitionKeyList != null) {
      size += RamUsageEstimator.shallowSizeOf(partitionKeyList);
      for (IDeviceID deviceID : partitionKeyList) {
        size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(deviceID);
      }
    }
    return size;
  }
}
