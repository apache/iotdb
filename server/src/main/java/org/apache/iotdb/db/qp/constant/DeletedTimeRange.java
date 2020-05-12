package org.apache.iotdb.db.qp.constant;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

/**
 * This class is used to describe interval deletion and is a closed interval [min, Max]
 */
public class DeletedTimeRange {

  private long minTime;
  private long maxTime;

  private Filter filter;

  public static class DeletedTimeRangeBuilder {

    private long minTime = Long.MIN_VALUE;
    private long maxTime = Long.MAX_VALUE;

    public DeletedTimeRangeBuilder setMaxTime(long maxTime) {
      this.maxTime = maxTime;
      return this;
    }

    public DeletedTimeRangeBuilder setMinTime(long minTime) {
      this.minTime = minTime;
      return this;
    }

    public DeletedTimeRange build() {
      if (minTime > maxTime) {
        throw new SQLParserException("Unreachable deleted time interval");
      }
      return new DeletedTimeRange(minTime, maxTime);
    }
  }

  public DeletedTimeRange(long minTime, long maxTime) {
    this.minTime = minTime;
    this.maxTime = maxTime;
    this.filter = FilterFactory.and(TimeFilter.gtEq(minTime), TimeFilter.ltEq(maxTime));
  }

  public boolean satisfy(long time) {
    return filter.satisfy(time, null);
  }

  public boolean containStartEndTime(long startTime, long endTime) {
    return filter.containStartEndTime(startTime, endTime);
  }

  public long getMinTime() {
    return minTime;
  }

  public void setMinTime(long minTime) {
    this.minTime = minTime;
  }

  public long getMaxTime() {
    return maxTime;
  }

  public void setMaxTime(long maxTime) {
    this.maxTime = maxTime;
  }

  public Filter getFilter() {
    return filter;
  }

  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  public void serialize(DataOutputStream stream) {

  }


  public static DeletedTimeRange deserialize(ByteBuffer buffer) {
    return null;
  }

  public void serialize(ByteBuffer buffer) {

  }
}
