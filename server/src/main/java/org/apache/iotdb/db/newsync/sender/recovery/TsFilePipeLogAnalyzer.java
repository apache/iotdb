package org.apache.iotdb.db.newsync.sender.recovery;

import org.apache.iotdb.db.engine.modification.Deletion;

import java.io.File;
import java.util.List;

public class TsFilePipeLogAnalyzer {
  public List<File> getRecoveryTsFiles() {
    return null;
  }

  public List<Deletion> getRecoveryDeletion() {
    return null;
  }

  public List<String> getRecoverySchema() {
    return null;
  }

  public boolean isCollectFinished() {
    return false;
  }
}
