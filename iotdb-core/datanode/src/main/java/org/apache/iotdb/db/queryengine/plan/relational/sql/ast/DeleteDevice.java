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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.execution.operator.schema.source.DeviceBlackListConstructor;
import org.apache.iotdb.db.queryengine.execution.operator.schema.source.TableDeviceQuerySource;
import org.apache.iotdb.db.queryengine.execution.relational.ColumnTransformerBuilder;
import org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;

public class DeleteDevice extends AbstractTraverseDevice {

  // Used for data deletion
  private List<TableDeletionEntry> modEntries;

  public DeleteDevice(final NodeLocation location, final Table table, final Expression where) {
    super(location, table, where);
    throw new SemanticException("Delete device is unsupported yet.");
  }

  public void parseModEntries(final TsTable table) {
    // TODO: Fallback to precise devices if modEnries parsing failure encountered
    modEntries = AnalyzeUtils.parseExpressions2ModEntries(where, table);
  }

  public void serializeModEntries(final DataOutputStream stream) throws IOException {
    if (Objects.nonNull(modEntries)) {
      ReadWriteIOUtils.write(modEntries.size(), stream);
      for (TableDeletionEntry modEntry : modEntries) {
        modEntry.serialize(stream);
      }
    }
  }

  public void serializePatternInfo(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getIdDeterminedFilterList().size(), stream);
    for (final List<SchemaFilter> filterList : idDeterminedFilterList) {
      ReadWriteIOUtils.write(filterList.size(), stream);
      for (final SchemaFilter filter : filterList) {
        SchemaFilter.serialize(filter, stream);
      }
    }
  }

  public void serializeFilterInfo(final DataOutputStream stream, final SessionInfo sessionInfo)
      throws IOException {
    ReadWriteIOUtils.write(idFuzzyPredicate == null ? (byte) 0 : (byte) 1, stream);
    if (idFuzzyPredicate != null) {
      Expression.serialize(idFuzzyPredicate, stream);
    }

    ReadWriteIOUtils.write(columnHeaderList.size(), stream);
    for (final ColumnHeader columnHeader : columnHeaderList) {
      columnHeader.serialize(stream);
    }

    ReadWriteIOUtils.write(Objects.nonNull(sessionInfo), stream);
    if (Objects.nonNull(sessionInfo)) {
      sessionInfo.serialize(stream);
    }
  }

  public static List<TableDeletionEntry> constructModEntries(final byte[] modInfo) {
    final ByteBuffer buffer = ByteBuffer.wrap(modInfo);

    final int size = ReadWriteIOUtils.readInt(buffer);
    final List<TableDeletionEntry> modEntries = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      modEntries.add((TableDeletionEntry) ModEntry.createFrom(buffer));
    }

    return modEntries;
  }

  public static List<PartialPath> constructPaths(
      final String database, final String tableName, final byte[] patternInfo) {
    final ByteBuffer buffer = ByteBuffer.wrap(patternInfo);

    // Device pattern list
    final int size = ReadWriteIOUtils.readInt(buffer);
    final List<List<SchemaFilter>> idDeterminedFilterList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      final int singleSize = ReadWriteIOUtils.readInt(buffer);
      idDeterminedFilterList.add(new ArrayList<>(singleSize));
      for (int k = 0; k < singleSize; k++) {
        idDeterminedFilterList.get(i).add(SchemaFilter.deserialize(buffer));
      }
    }

    return TableDeviceQuerySource.getDevicePatternList(database, tableName, idDeterminedFilterList);
  }

  public static DeviceBlackListConstructor constructDevicePredicateUpdater(
      final String database,
      final String tableName,
      final byte[] filterInfo,
      final BiFunction<Integer, String, Binary> attributeProvider,
      final MemSchemaRegionStatistics regionStatistics) {
    final ByteBuffer buffer = ByteBuffer.wrap(filterInfo);

    Expression predicate = null;
    if (buffer.get() == 1) {
      predicate = Expression.deserialize(buffer);
    }

    final int size = ReadWriteIOUtils.readInt(buffer);
    final List<ColumnHeader> columnHeaderList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      columnHeaderList.add(ColumnHeader.deserialize(buffer));
    }

    SessionInfo sessionInfo = null;
    if (ReadWriteIOUtils.readBool(buffer)) {
      sessionInfo = SessionInfo.deserializeFrom(buffer);
    }

    final AtomicInteger valueColumnIndex = new AtomicInteger(0);
    final Map<Symbol, List<InputLocation>> inputLocations =
        columnHeaderList.stream()
            .collect(
                Collectors.toMap(
                    columnHeader -> new Symbol(columnHeader.getColumnName()),
                    columnHeader ->
                        Collections.singletonList(
                            new InputLocation(0, valueColumnIndex.getAndIncrement()))));

    final TypeProvider mockTypeProvider =
        new TypeProvider(
            columnHeaderList.stream()
                .collect(
                    Collectors.toMap(
                        columnHeader -> new Symbol(columnHeader.getColumnName()),
                        columnHeader -> TypeFactory.getType(columnHeader.getColumnType()))));
    final Metadata metadata = LocalExecutionPlanner.getInstance().metadata;

    // records LeafColumnTransformer of filter
    final List<LeafColumnTransformer> filterLeafColumnTransformerList = new ArrayList<>();

    // records subexpression -> ColumnTransformer for filter
    final Map<Expression, ColumnTransformer> filterExpressionColumnTransformerMap = new HashMap<>();

    final ColumnTransformerBuilder visitor = new ColumnTransformerBuilder();

    final ColumnTransformer filterOutputTransformer =
        Objects.nonNull(predicate)
            ? visitor.process(
                predicate,
                new ColumnTransformerBuilder.Context(
                    sessionInfo,
                    filterLeafColumnTransformerList,
                    inputLocations,
                    filterExpressionColumnTransformerMap,
                    ImmutableMap.of(),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    0,
                    mockTypeProvider,
                    metadata))
            : null;

    return new DeviceBlackListConstructor(
        filterLeafColumnTransformerList,
        filterOutputTransformer,
        database,
        tableName,
        columnHeaderList,
        attributeProvider,
        regionStatistics);
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitDeleteDevice(this, context);
  }

  @Override
  public String toString() {
    return toStringHelper(this) + " - " + super.toStringContent();
  }
}
