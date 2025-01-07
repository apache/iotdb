package org.apache.iotdb.confignode.consensus.request.write.table;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.enums.TSDataType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class AlterColumnDataTypePlan extends AbstractTableColumnPlan {

  private TSDataType newType;

  public AlterColumnDataTypePlan() {
    super(ConfigPhysicalPlanType.AlterColumnDataType);
  }

  public AlterColumnDataTypePlan(
      String database, String tableName, String columnName, TSDataType newType) {
    super(ConfigPhysicalPlanType.AlterColumnDataType, database, tableName, columnName);
    this.newType = newType;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    super.serializeImpl(stream);
    stream.writeInt(newType.serialize());
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    super.deserializeImpl(buffer);
    newType = TSDataType.deserializeFrom(buffer);
  }

  public void setNewType(TSDataType newType) {
    this.newType = newType;
  }

  public TSDataType getNewType() {
    return newType;
  }
}
