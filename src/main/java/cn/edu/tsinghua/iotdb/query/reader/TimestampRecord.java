package cn.edu.tsinghua.iotdb.query.reader;

import java.util.List;

/**
 * <p>
 * This class storage the timestamps and its time index,
 * may it can be replaced by queue.
 * </p>
 *
 * @author CGF
 */
public class TimestampRecord {
   private List<Long> timestamps;
   private int timeIndex;

   public TimestampRecord(List<Long> timestamps, int timeIndex) {
       this.timestamps = timestamps;
       this.timeIndex = timeIndex;
   }

    public List<Long> getTimestamps() {
        return timestamps;
    }

    public int getTimeIndex() {
        return timeIndex;
    }

    public void setTimestamps(List<Long> timestamps) {
        this.timestamps = timestamps;
    }

    public void setTimeIndex(int timeIndex) {
        this.timeIndex = timeIndex;
    }
}
