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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.table;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.SchemaQueryScanNode;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TableDeviceScanNode extends SchemaQueryScanNode {

  private String database;

  private String tableName;

  private List<SchemaFilter> idDeterminedFilterList;

  private List<SchemaFilter> idFuzzyFilterList;

  private List<ColumnHeader> columnHeaderList;

  private TRegionReplicaSet schemaRegionReplicaSet;

  public TableDeviceScanNode(PlanNodeId id) {
    super(id);
  }

  public TableDeviceScanNode(
      PlanNodeId id,
      String database,
      String tableName,
      List<SchemaFilter> idDeterminedFilterList,
      List<SchemaFilter> idFuzzyFilterList,
      List<ColumnHeader> columnHeaderList,
      TRegionReplicaSet regionReplicaSet) {
    super(id);
    this.database = database;
    this.tableName = tableName;
    this.idDeterminedFilterList = idDeterminedFilterList;
    this.idFuzzyFilterList = idFuzzyFilterList;
    this.columnHeaderList = columnHeaderList;
    this.schemaRegionReplicaSet = regionReplicaSet;
  }

  public String getDatabase() {
    return database;
  }

  public String getTableName() {
    return tableName;
  }

  public List<SchemaFilter> getIdDeterminedFilterList() {
    return idDeterminedFilterList;
  }

  public List<SchemaFilter> getIdFuzzyFilterList() {
    return idFuzzyFilterList;
  }

  public List<ColumnHeader> getColumnHeaderList() {
    return columnHeaderList;
  }

  @Override
  public void open() throws Exception {}

  @Override
  public void setRegionReplicaSet(TRegionReplicaSet regionReplicaSet) {
    this.schemaRegionReplicaSet = regionReplicaSet;
  }

  @Override
  public void close() throws Exception {}

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return schemaRegionReplicaSet;
  }

  @Override
  public PlanNode clone() {
    return new TableDeviceScanNode(
        getPlanNodeId(),
        database,
        tableName,
        idDeterminedFilterList,
        idFuzzyFilterList,
        columnHeaderList,
        schemaRegionReplicaSet);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return columnHeaderList.stream().map(ColumnHeader::getColumnName).collect(Collectors.toList());
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_DEVICE_SCAN.serialize(byteBuffer);
    ReadWriteIOUtils.write(database, byteBuffer);
    ReadWriteIOUtils.write(tableName, byteBuffer);

    ReadWriteIOUtils.write(idDeterminedFilterList.size(), byteBuffer);
    for (SchemaFilter schemaFilter : idDeterminedFilterList) {
      SchemaFilter.serialize(schemaFilter, byteBuffer);
    }

    ReadWriteIOUtils.write(idFuzzyFilterList.size(), byteBuffer);
    for (SchemaFilter schemaFilter : idFuzzyFilterList) {
      SchemaFilter.serialize(schemaFilter, byteBuffer);
    }

    ReadWriteIOUtils.write(columnHeaderList.size(), byteBuffer);
    for (ColumnHeader columnHeader : columnHeaderList) {
      columnHeader.serialize(byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_DEVICE_SCAN.serialize(stream);
    ReadWriteIOUtils.write(database, stream);
    ReadWriteIOUtils.write(tableName, stream);

    ReadWriteIOUtils.write(idDeterminedFilterList.size(), stream);
    for (SchemaFilter schemaFilter : idDeterminedFilterList) {
      SchemaFilter.serialize(schemaFilter, stream);
    }

    ReadWriteIOUtils.write(idFuzzyFilterList.size(), stream);
    for (SchemaFilter schemaFilter : idFuzzyFilterList) {
      SchemaFilter.serialize(schemaFilter, stream);
    }

    ReadWriteIOUtils.write(columnHeaderList.size(), stream);
    for (ColumnHeader columnHeader : columnHeaderList) {
      columnHeader.serialize(stream);
    }
  }

  public static TableDeviceScanNode deserialize(ByteBuffer buffer) {
    String database = ReadWriteIOUtils.readString(buffer);
    String tableName = ReadWriteIOUtils.readString(buffer);

    int size = ReadWriteIOUtils.readInt(buffer);
    List<SchemaFilter> idDeterminedFilterList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      idDeterminedFilterList.add(SchemaFilter.deserialize(buffer));
    }

    size = ReadWriteIOUtils.readInt(buffer);
    List<SchemaFilter> idFuzzyFilterList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      idFuzzyFilterList.add(SchemaFilter.deserialize(buffer));
    }

    size = ReadWriteIOUtils.readInt(buffer);
    List<ColumnHeader> columnHeaderList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      columnHeaderList.add(ColumnHeader.deserialize(buffer));
    }

    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new TableDeviceScanNode(
        planNodeId,
        database,
        tableName,
        idDeterminedFilterList,
        idFuzzyFilterList,
        columnHeaderList,
        null);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitTableDeviceScan(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TableDeviceScanNode)) return false;
    if (!super.equals(o)) return false;
    TableDeviceScanNode that = (TableDeviceScanNode) o;
    return Objects.equals(database, that.database)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(idDeterminedFilterList, that.idDeterminedFilterList)
        && Objects.equals(idFuzzyFilterList, that.idFuzzyFilterList)
        && Objects.equals(columnHeaderList, that.columnHeaderList)
        && Objects.equals(schemaRegionReplicaSet, that.schemaRegionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        database,
        tableName,
        idDeterminedFilterList,
        idFuzzyFilterList,
        columnHeaderList,
        schemaRegionReplicaSet);
  }
}
