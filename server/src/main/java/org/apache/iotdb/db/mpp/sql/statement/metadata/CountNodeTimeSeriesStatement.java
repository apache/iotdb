package org.apache.iotdb.db.mpp.sql.statement.metadata;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.statement.StatementVisitor;

public class CountNodeTimeSeriesStatement extends CountStatement {
  private int level;

  public CountNodeTimeSeriesStatement(PartialPath partialPath, int level) {
    super(partialPath);
    this.level = level;
  }

  public int getLevel() {
    return level;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCountNodeTimeSeries(this, context);
  }
}
