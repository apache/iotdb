package cn.edu.tsinghua.tsfile.timeseries.write.record;

import cn.edu.tsinghua.tsfile.timeseries.utils.StringContainer;

import java.util.ArrayList;
import java.util.List;

/**
 * TSRecord is a kind of format that TSFile receives.TSRecord contains timestamp, deltaObjectId and
 * a list of data points.
 *
 * @author kangrong
 */
public class TSRecord {
    public long time;
    public String deltaObjectId;
    public List<DataPoint> dataPointList = new ArrayList<>();

    public TSRecord(long timestamp, String deltaObjectId) {
        this.time = timestamp;
        this.deltaObjectId = deltaObjectId;
    }

    public void setTime(long timestamp) {
        this.time = timestamp;
    }

    public void addTuple(DataPoint tuple) {
        this.dataPointList.add(tuple);
    }

    public String toString() {
        StringContainer sc = new StringContainer(" ");
        sc.addTail("{delta object id:", deltaObjectId, "time:", time, ",data:[");
        for (DataPoint tuple : dataPointList) {
            sc.addTail(tuple);
        }
        sc.addTail("]}");
        return sc.toString();
    }
}
