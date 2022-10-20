package org.apache.iotdb.confignode.writelog.io;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author 辰行
 * @date 2022/10/20
 */
public class BatchLogReader implements ILogReader {
  private static Logger logger = LoggerFactory.getLogger(BatchLogReader.class);

  private Iterator<ConfigPhysicalPlan> planIterator;

  private boolean fileCorrupted = false;

  BatchLogReader(ByteBuffer buffer) {
    List<ConfigPhysicalPlan> logs = readLogs(buffer);
    this.planIterator = logs.iterator();
  }

  private List<ConfigPhysicalPlan> readLogs(ByteBuffer buffer) {
    List<ConfigPhysicalPlan> plans = new ArrayList<>();
    while (buffer.position() != buffer.limit()) {
      try {
        plans.add(ConfigPhysicalPlan.Factory.create(buffer));
      } catch (IOException e) {
        logger.error("Cannot deserialize PhysicalPlans from ByteBuffer, ignore remaining logs", e);
        fileCorrupted = true;
        break;
      }
    }
    return plans;
  }

  @Override
  public void close() {
    // nothing to be closed
  }

  @Override
  public boolean hasNext() {
    return planIterator.hasNext();
  }

  @Override
  public ConfigPhysicalPlan next() {
    return planIterator.next();
  }

  public boolean isFileCorrupted() {
    return fileCorrupted;
  }
}
