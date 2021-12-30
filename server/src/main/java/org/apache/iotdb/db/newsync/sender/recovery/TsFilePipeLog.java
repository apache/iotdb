package org.apache.iotdb.db.newsync.sender.recovery;

import org.apache.iotdb.db.engine.modification.Deletion;

import java.io.File;

public class TsFilePipeLog {
  public void addNewTsFile(File tsfile) {}

  public void addNewDeletion(Deletion deletion) {}

  public void addNewSchema(String schema) {}

  public void finishCollect() {}
}
