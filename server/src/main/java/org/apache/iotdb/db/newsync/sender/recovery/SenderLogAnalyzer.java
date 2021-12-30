package org.apache.iotdb.db.newsync.sender.recovery;

import org.apache.iotdb.db.newsync.sender.pipe.Pipe;
import org.apache.iotdb.db.newsync.sender.pipe.PipeSink;

import java.util.List;
import java.util.Map;

public class SenderLogAnalyzer {
  public Map<String, PipeSink> getRecoveryPipeSinks() {
    return null;
  }

  public List<Pipe> getRecoveryPipes() {
    return null;
  }

  public Pipe getRecoveryRunningPipe() {
    return null;
  }
}
