package org.apache.iotdb.db.queryengine.plan.statement.table;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;

public class CreateTableDeviceStatement extends Statement {

  private final String database;

  private final String table;

  private final List<String[]> deviceIdList;

  private final List<String> attributeNameList;

  private final List<List<String>> attributeValueList;

  private transient List<PartialPath> devicePathList;

  public CreateTableDeviceStatement(
      String database,
      String table,
      List<String[]> deviceIdList,
      List<String> attributeNameList,
      List<List<String>> attributeValueList) {
    this.database = database;
    this.table = table;
    this.deviceIdList = deviceIdList;
    this.attributeNameList = attributeNameList;
    this.attributeValueList = attributeValueList;
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  public List<String[]> getDeviceIdList() {
    return deviceIdList;
  }

  public List<String> getAttributeNameList() {
    return attributeNameList;
  }

  public List<List<String>> getAttributeValueList() {
    return attributeValueList;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCreateTableDevice(this, context);
  }

  @Override
  public List<PartialPath> getPaths() {
    if (devicePathList == null) {
      generateDevicePaths();
    }
    return devicePathList;
  }

  private void generateDevicePaths() {
    List<PartialPath> result = new ArrayList<>(deviceIdList.size());
    for (String[] deviceId : deviceIdList) {
      String[] nodes = new String[3 + deviceId.length];
      nodes[0] = PATH_ROOT;
      nodes[1] = database;
      nodes[2] = table;
      System.arraycopy(deviceId, 0, nodes, 3, deviceId.length);
      result.add(new PartialPath(nodes));
    }
    this.devicePathList = result;
  }
}
