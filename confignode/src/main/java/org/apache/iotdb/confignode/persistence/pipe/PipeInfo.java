package org.apache.iotdb.confignode.persistence.pipe;

import org.apache.iotdb.commons.snapshot.SnapshotProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class PipeInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeInfo.class);

  private PipePluginInfo pipePluginInfo;

  public PipeInfo() throws IOException {
    pipePluginInfo = new PipePluginInfo();
  }

  public PipePluginInfo getPipePluginInfo() {
    return pipePluginInfo;
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    return pipePluginInfo.processTakeSnapshot(snapshotDir);
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    pipePluginInfo.processLoadSnapshot(snapshotDir);
  }
}
