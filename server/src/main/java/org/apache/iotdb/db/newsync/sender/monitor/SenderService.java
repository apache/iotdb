package org.apache.iotdb.db.newsync.sender.monitor;

import org.apache.iotdb.db.newsync.sender.pipe.Pipe;
import org.apache.iotdb.db.newsync.sender.pipe.PipeSink;

import java.util.List;
import java.util.Map;

public class SenderService {
  private Map<String, PipeSink> pipeSinks;
  private List<Pipe> pipes;

  private Pipe runningPipe;

  public void addPipeSink(PipeSink pipeSink) {
  }

  public void dropPipeSink(String name) {
  }
}
