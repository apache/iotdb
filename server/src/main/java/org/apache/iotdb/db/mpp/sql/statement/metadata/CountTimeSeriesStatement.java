package org.apache.iotdb.db.mpp.sql.statement.metadata;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.statement.StatementVisitor;

public class CountTimeSeriesStatement extends CountStatement {
  public CountTimeSeriesStatement(PartialPath partialPath) {
    super(partialPath);
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCountTimeSeries(this, context);
  }
}
