/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SourceNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.MEASUREMENT;

public class TableScanNode extends SourceNode {

  protected final QualifiedObjectName qualifiedObjectName;
  // Indicate the column this node need to output
  protected List<Symbol> outputSymbols;
  // Indicate the column this node need to fetch from StorageEngine,
  // the number of fetched columns may be more than output columns when there are predicates push
  // down.
  protected Map<Symbol, ColumnSchema> assignments;

  protected List<DeviceEntry> deviceEntries;

  // Indicates the respective index order of ID and Attribute columns in DeviceEntry.
  // For example, for DeviceEntry `table1.tag1.tag2.attribute1.attribute2.s1.s2`, the content of
  // `idAndAttributeIndexMap` will
  // be `tag1: 0, tag2: 1, attribute1: 0, attribute2: 1`.
  protected final Map<Symbol, Integer> idAndAttributeIndexMap;

  // The order to traverse the data.
  // Currently, we only support TIMESTAMP_ASC and TIMESTAMP_DESC here.
  // The default order is TIMESTAMP_ASC, which means "order by timestamp asc"
  protected Ordering scanOrder = Ordering.ASC;

  // extracted time filter expression in where clause
  // case 1: where s1 > 1 and time >= 0 and time <= 10, time predicate will be time >= 0 and time <=
  // 10, pushDownPredicate will be s1 > 1
  // case 2: where s1 > 1 or time < 10, time predicate will be null, pushDownPredicate will be s1 >
  // 1 or time < 10
  @Nullable protected Expression timePredicate;

  protected Filter timeFilter;

  // push down predicate for current series, could be null if it doesn't exist
  @Nullable protected Expression pushDownPredicate;

  // push down limit for result set. The default value is -1, which means no limit
  protected long pushDownLimit;

  // push down offset for result set. The default value is 0
  protected long pushDownOffset;

  // pushLimitToEachDevice == true means that each device in TableScanNode need to return
  // `pushDownLimit` row number
  // pushLimitToEachDevice == false means that all devices in TableScanNode totally need to return
  // `pushDownLimit` row number
  protected boolean pushLimitToEachDevice = false;

  // The id of DataRegion where the node will run
  protected TRegionReplicaSet regionReplicaSet;

  public TableScanNode(
      PlanNodeId id,
      QualifiedObjectName qualifiedObjectName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      Map<Symbol, Integer> idAndAttributeIndexMap) {
    super(id);
    this.qualifiedObjectName = qualifiedObjectName;
    this.outputSymbols = outputSymbols;
    this.assignments = assignments;
    this.idAndAttributeIndexMap = idAndAttributeIndexMap;
  }

  public TableScanNode(
      PlanNodeId id,
      QualifiedObjectName qualifiedObjectName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      List<DeviceEntry> deviceEntries,
      Map<Symbol, Integer> idAndAttributeIndexMap,
      Ordering scanOrder,
      Expression timePredicate,
      Expression pushDownPredicate,
      long pushDownLimit,
      long pushDownOffset,
      boolean pushLimitToEachDevice) {
    super(id);
    this.qualifiedObjectName = qualifiedObjectName;
    this.outputSymbols = outputSymbols;
    this.assignments = assignments;
    this.deviceEntries = deviceEntries;
    this.idAndAttributeIndexMap = idAndAttributeIndexMap;
    this.scanOrder = scanOrder;
    this.timePredicate = timePredicate;
    this.pushDownPredicate = pushDownPredicate;
    this.pushDownLimit = pushDownLimit;
    this.pushDownOffset = pushDownOffset;
    this.pushLimitToEachDevice = pushLimitToEachDevice;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitTableScan(this, context);
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public TableScanNode clone() {
    return new TableScanNode(
        getPlanNodeId(),
        qualifiedObjectName,
        outputSymbols,
        assignments,
        deviceEntries,
        idAndAttributeIndexMap,
        scanOrder,
        timePredicate,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        pushLimitToEachDevice);
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputSymbols.stream().map(Symbol::getName).collect(Collectors.toList());
  }

  public List<Symbol> getIdColumnsInTableStore(Metadata metadata, SessionInfo session) {
    return Objects.requireNonNull(
            metadata.getTableSchema(session, qualifiedObjectName).orElse(null))
        .getColumns()
        .stream()
        .filter(columnSchema -> columnSchema.getColumnCategory() == TsTableColumnCategory.ID)
        .map(columnSchema -> Symbol.of(columnSchema.getName()))
        .collect(Collectors.toList());
  }

  public boolean isMeasurementColumn(Symbol symbol) {
    ColumnSchema columnSchema = assignments.get(symbol);
    return columnSchema != null && columnSchema.getColumnCategory() == MEASUREMENT;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_SCAN_NODE.serialize(byteBuffer);

    if (qualifiedObjectName.getDatabaseName() != null) {
      ReadWriteIOUtils.write(true, byteBuffer);
      ReadWriteIOUtils.write(qualifiedObjectName.getDatabaseName(), byteBuffer);
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }
    ReadWriteIOUtils.write(qualifiedObjectName.getObjectName(), byteBuffer);

    ReadWriteIOUtils.write(outputSymbols.size(), byteBuffer);
    outputSymbols.forEach(symbol -> ReadWriteIOUtils.write(symbol.getName(), byteBuffer));

    ReadWriteIOUtils.write(assignments.size(), byteBuffer);
    for (Map.Entry<Symbol, ColumnSchema> entry : assignments.entrySet()) {
      Symbol.serialize(entry.getKey(), byteBuffer);
      ColumnSchema.serialize(entry.getValue(), byteBuffer);
    }

    ReadWriteIOUtils.write(deviceEntries.size(), byteBuffer);
    for (DeviceEntry entry : deviceEntries) {
      entry.serialize(byteBuffer);
    }

    ReadWriteIOUtils.write(idAndAttributeIndexMap.size(), byteBuffer);
    for (Map.Entry<Symbol, Integer> entry : idAndAttributeIndexMap.entrySet()) {
      Symbol.serialize(entry.getKey(), byteBuffer);
      ReadWriteIOUtils.write(entry.getValue(), byteBuffer);
    }

    ReadWriteIOUtils.write(scanOrder.ordinal(), byteBuffer);

    if (timePredicate != null) {
      ReadWriteIOUtils.write(true, byteBuffer);
      Expression.serialize(timePredicate, byteBuffer);
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }

    if (pushDownPredicate != null) {
      ReadWriteIOUtils.write(true, byteBuffer);
      Expression.serialize(pushDownPredicate, byteBuffer);
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }

    ReadWriteIOUtils.write(pushDownLimit, byteBuffer);
    ReadWriteIOUtils.write(pushDownOffset, byteBuffer);
    ReadWriteIOUtils.write(pushLimitToEachDevice, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_SCAN_NODE.serialize(stream);
    if (qualifiedObjectName.getDatabaseName() != null) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(qualifiedObjectName.getDatabaseName(), stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
    ReadWriteIOUtils.write(qualifiedObjectName.getObjectName(), stream);

    ReadWriteIOUtils.write(outputSymbols.size(), stream);
    for (Symbol symbol : outputSymbols) {
      ReadWriteIOUtils.write(symbol.getName(), stream);
    }

    ReadWriteIOUtils.write(assignments.size(), stream);
    for (Map.Entry<Symbol, ColumnSchema> entry : assignments.entrySet()) {
      Symbol.serialize(entry.getKey(), stream);
      ColumnSchema.serialize(entry.getValue(), stream);
    }

    ReadWriteIOUtils.write(deviceEntries.size(), stream);
    for (DeviceEntry entry : deviceEntries) {
      entry.serialize(stream);
    }

    ReadWriteIOUtils.write(idAndAttributeIndexMap.size(), stream);
    for (Map.Entry<Symbol, Integer> entry : idAndAttributeIndexMap.entrySet()) {
      Symbol.serialize(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }

    ReadWriteIOUtils.write(scanOrder.ordinal(), stream);

    if (timePredicate != null) {
      ReadWriteIOUtils.write(true, stream);
      Expression.serialize(timePredicate, stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    if (pushDownPredicate != null) {
      ReadWriteIOUtils.write(true, stream);
      Expression.serialize(pushDownPredicate, stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    ReadWriteIOUtils.write(pushDownLimit, stream);
    ReadWriteIOUtils.write(pushDownOffset, stream);
    ReadWriteIOUtils.write(pushLimitToEachDevice, stream);
  }

  public static TableScanNode deserialize(ByteBuffer byteBuffer) {
    boolean hasDatabaseName = ReadWriteIOUtils.readBool(byteBuffer);
    String databaseName = null;
    if (hasDatabaseName) {
      databaseName = ReadWriteIOUtils.readString(byteBuffer);
    }
    String tableName = ReadWriteIOUtils.readString(byteBuffer);
    QualifiedObjectName qualifiedObjectName = new QualifiedObjectName(databaseName, tableName);

    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<Symbol> outputSymbols = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      outputSymbols.add(Symbol.deserialize(byteBuffer));
    }

    size = ReadWriteIOUtils.readInt(byteBuffer);
    Map<Symbol, ColumnSchema> assignments = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      assignments.put(Symbol.deserialize(byteBuffer), ColumnSchema.deserialize(byteBuffer));
    }

    size = ReadWriteIOUtils.readInt(byteBuffer);
    List<DeviceEntry> deviceEntries = new ArrayList<>(size);
    while (size-- > 0) {
      deviceEntries.add(DeviceEntry.deserialize(byteBuffer));
    }

    size = ReadWriteIOUtils.readInt(byteBuffer);
    Map<Symbol, Integer> idAndAttributeIndexMap = new HashMap<>(size);
    while (size-- > 0) {
      idAndAttributeIndexMap.put(
          Symbol.deserialize(byteBuffer), ReadWriteIOUtils.readInt(byteBuffer));
    }

    Ordering scanOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];

    Expression timePredicate = null;
    boolean hasTimePredicate = ReadWriteIOUtils.readBool(byteBuffer);
    if (hasTimePredicate) {
      timePredicate = Expression.deserialize(byteBuffer);
    }

    Expression pushDownPredicate = null;
    boolean hasPushDownPredicate = ReadWriteIOUtils.readBool(byteBuffer);
    if (hasPushDownPredicate) {
      pushDownPredicate = Expression.deserialize(byteBuffer);
    }

    long pushDownLimit = ReadWriteIOUtils.readLong(byteBuffer);
    long pushDownOffset = ReadWriteIOUtils.readLong(byteBuffer);
    boolean pushLimitToEachDevice = ReadWriteIOUtils.readBool(byteBuffer);

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);

    return new TableScanNode(
        planNodeId,
        qualifiedObjectName,
        outputSymbols,
        assignments,
        deviceEntries,
        idAndAttributeIndexMap,
        scanOrder,
        timePredicate,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        pushLimitToEachDevice);
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return outputSymbols;
  }

  @Override
  public void open() throws Exception {}

  @Override
  public void close() throws Exception {}

  public QualifiedObjectName getQualifiedObjectName() {
    return this.qualifiedObjectName;
  }

  public void setOutputSymbols(List<Symbol> outputSymbols) {
    this.outputSymbols = outputSymbols;
  }

  public void setAssignments(Map<Symbol, ColumnSchema> assignments) {
    this.assignments = assignments;
  }

  public Map<Symbol, ColumnSchema> getAssignments() {
    return this.assignments;
  }

  public void setDeviceEntries(List<DeviceEntry> deviceEntries) {
    this.deviceEntries = deviceEntries;
  }

  public Map<Symbol, Integer> getIdAndAttributeIndexMap() {
    return this.idAndAttributeIndexMap;
  }

  public void setScanOrder(Ordering scanOrder) {
    this.scanOrder = scanOrder;
  }

  public Ordering getScanOrder() {
    return this.scanOrder;
  }

  public List<DeviceEntry> getDeviceEntries() {
    return deviceEntries;
  }

  public void appendDeviceEntry(DeviceEntry deviceEntry) {
    this.deviceEntries.add(deviceEntry);
  }

  public long getPushDownLimit() {
    return this.pushDownLimit;
  }

  public void setPushDownLimit(long pushDownLimit) {
    this.pushDownLimit = pushDownLimit;
  }

  public long getPushDownOffset() {
    return this.pushDownOffset;
  }

  public void setPushDownOffset(long pushDownOffset) {
    this.pushDownOffset = pushDownOffset;
  }

  public Expression getPushDownPredicate() {
    return this.pushDownPredicate;
  }

  public void setPushLimitToEachDevice(boolean pushLimitToEachDevice) {
    this.pushLimitToEachDevice = pushLimitToEachDevice;
  }

  public void setPushDownPredicate(@Nullable Expression pushDownPredicate) {
    this.pushDownPredicate = pushDownPredicate;
  }

  public boolean isPushLimitToEachDevice() {
    return pushLimitToEachDevice;
  }

  public TRegionReplicaSet getRegionReplicaSet() {
    return this.regionReplicaSet;
  }

  public void setRegionReplicaSet(TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
  }

  public Optional<Expression> getTimePredicate() {
    return Optional.ofNullable(timePredicate);
  }

  public void setTimePredicate(@Nullable Expression timePredicate) {
    this.timePredicate = timePredicate;
  }

  public Filter getTimeFilter() {
    return timeFilter;
  }

  public void setTimeFilter(Filter timeFilter) {
    this.timeFilter = timeFilter;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    TableScanNode that = (TableScanNode) o;
    return Objects.equals(qualifiedObjectName, that.qualifiedObjectName)
        && Objects.equals(outputSymbols, that.outputSymbols)
        && Objects.equals(regionReplicaSet, that.regionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), qualifiedObjectName, outputSymbols, regionReplicaSet);
  }

  @Override
  public String toString() {
    return "TableScanNode-" + this.getPlanNodeId();
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    checkArgument(newChildren.isEmpty(), "newChildren is not empty");
    return this;
  }
}
