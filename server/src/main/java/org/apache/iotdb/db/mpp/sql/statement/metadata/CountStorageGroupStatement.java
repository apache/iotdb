package org.apache.iotdb.db.mpp.sql.statement.metadata;

import org.apache.iotdb.db.metadata.path.PartialPath;

public class CountStorageGroupStatement extends CountStatement {
  private PartialPath partialPath;

  public CountStorageGroupStatement(PartialPath partialPath) {
    super(partialPath);
  }
}
