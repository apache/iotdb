package org.apache.iotdb.db.mpp.sql.statement.metadata;

import org.apache.iotdb.db.metadata.path.PartialPath;

public class CountNodesStatement extends CountStatement {
  private final int level;

  public CountNodesStatement(PartialPath partialPath, int level) {
    super(partialPath);
    this.level = level;
  }
}
