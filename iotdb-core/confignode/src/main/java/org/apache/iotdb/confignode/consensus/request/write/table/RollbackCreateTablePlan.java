package org.apache.iotdb.confignode.consensus.request.write.table;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RollbackCreateTablePlan extends ConfigPhysicalPlan {

  private String database;

  private String tableName;

  public RollbackCreateTablePlan() {
    super(ConfigPhysicalPlanType.RollbackCreateTable);
  }

  public RollbackCreateTablePlan(String database, String tableName) {
    super(ConfigPhysicalPlanType.RollbackCreateTable);
    this.database = database;
    this.tableName = tableName;
  }

  public String getDatabase() {
    return database;
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(database, stream);
    ReadWriteIOUtils.write(tableName, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.database = ReadWriteIOUtils.readString(buffer);
    this.tableName = ReadWriteIOUtils.readString(buffer);
  }
}
