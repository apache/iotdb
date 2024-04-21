package org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
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
  protected void serializeAttributes(ByteBuffer byteBuffer) {}

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {}

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
