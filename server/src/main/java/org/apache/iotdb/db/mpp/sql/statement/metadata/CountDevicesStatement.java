package org.apache.iotdb.db.mpp.sql.statement.metadata;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.statement.StatementVisitor;

public class CountDevicesStatement extends CountStatement {
  public CountDevicesStatement(PartialPath partialPath) {
    super(partialPath);
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCountDevices(this, context);
  }
}
