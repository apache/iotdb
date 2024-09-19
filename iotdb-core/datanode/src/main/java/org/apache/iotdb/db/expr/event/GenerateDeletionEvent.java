package org.apache.iotdb.db.expr.event;

import org.apache.iotdb.db.expr.conf.SimulationConfig;
import org.apache.iotdb.db.expr.entity.SimDeletion;
import org.apache.iotdb.db.expr.entity.SimModFile;
import org.apache.iotdb.db.expr.simulator.SimulationContext;

import org.apache.tsfile.read.common.TimeRange;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class GenerateDeletionEvent extends Event {

  public SimDeletion currentDeletion;
  public long step;
  private final Supplier<Long> intervalGenerator;

  public Collection<SimModFile> involvedModFiles;

  public GenerateDeletionEvent(
      SimulationConfig config,
      SimDeletion currentDeletion,
      long step,
      Supplier<Long> intervalGenerator) {
    super(config);
    
    this.currentDeletion = currentDeletion;
    this.step = step;
    this.intervalGenerator = intervalGenerator;
  }

  public SimDeletion nextDeletion() {
    return new SimDeletion(
        new TimeRange(
            currentDeletion.timeRange.getMin() + step, currentDeletion.timeRange.getMax() + step));
  }

  @Override
  public List<Event> nextEvents(SimulationContext context) {
    GenerateDeletionEvent event =
        new GenerateDeletionEvent(config, nextDeletion(), step, intervalGenerator);
    event.generateTimestamp =
        context.getSimulator().getCurrentTimestamp() + intervalGenerator.get();
    return Collections.singletonList(event);
  }

  @Override
  public long calTimeConsumption() {
    double sum = 0.0;
    sum +=
        involvedModFiles.size() * config.IoSeekTimestamp
            + 1.0
                * involvedModFiles.size()
                * config.deletionSizeInByte
                / config.IoBandwidthBytesPerTimestamp;
    return Math.round(sum);
  }
}
