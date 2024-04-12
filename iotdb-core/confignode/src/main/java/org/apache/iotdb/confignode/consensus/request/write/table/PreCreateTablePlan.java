package org.apache.iotdb.confignode.consensus.request.write.table;

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class PreCreateTablePlan extends ConfigPhysicalPlan {

  private String database;

  private TsTable table;

  public PreCreateTablePlan() {
    super(ConfigPhysicalPlanType.PreCreateTable);
  }

  public PreCreateTablePlan(String database, TsTable table) {
    super(ConfigPhysicalPlanType.PreCreateTable);
    this.database = database;
    this.table = table;
  }

  public String getDatabase() {
    return database;
  }

  public TsTable getTable() {
    return table;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(database, stream);
    table.serialize(stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    database = ReadWriteIOUtils.readString(buffer);
    table = TsTable.deserialize(buffer);
  }
}
