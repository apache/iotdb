package org.apache.iotdb.db.queryengine.plan.statement.table;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;

import java.util.Collections;
import java.util.List;

public class FetchTableDevicesStatement extends Statement {

  private final String database;

  private final String tableName;

  private final List<String[]> deviceIdList;

  public FetchTableDevicesStatement(
      String database, String tableName, List<String[]> deviceIdList) {
    this.database = database;
    this.tableName = tableName;
    this.deviceIdList = deviceIdList;
  }

  public String getDatabase() {
    return database;
  }

  public String getTableName() {
    return tableName;
  }

  public List<String[]> getDeviceIdList() {
    return deviceIdList;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }
}
