package org.apache.iotdb.tsfile.write.record;

import org.apache.iotdb.tsfile.utils.StringContainer;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;

import java.util.ArrayList;
import java.util.List;

/**
 * TSRecord is a kind of format that TsFile receives.TSRecord contains timestamp, deviceId and
 * a list of data points.
 *
 * @author kangrong
 */
public class TSRecord {
    /** timestamp of this TSRecord **/
    public long time;
    /** deviceId of this TSRecord **/
    public String deviceId;
    /** all value of this TSRecord **/
    public List<DataPoint> dataPointList = new ArrayList<>();

    /**
     * constructor of TSRecord
     * @param timestamp timestamp of this TSRecord
     * @param deviceId deviceId of this TSRecord
     */
    public TSRecord(long timestamp, String deviceId) {
        this.time = timestamp;
        this.deviceId = deviceId;
    }

    public void setTime(long timestamp) {
        this.time = timestamp;
    }

    /**
     * add one data point to this TSRecord
     * @param tuple data point to be added
     */
    public void addTuple(DataPoint tuple) {
        this.dataPointList.add(tuple);
    }

    /**
     * output this TSRecord in String format.For example:
     * {device id: d1 time: 123456 ,data:[
     *      {measurement id: s1 type: INT32 value: 1 }
     *      {measurement id: s2 type: FLOAT value: 11.11 }
     *      {measurement id: s3 type: BOOLEAN value: true }
     * ]}
     * @return the String format of this TSRecord
     */
    @Override
    public String toString() {
        StringContainer sc = new StringContainer(" ");
        sc.addTail("{device id:", deviceId, "time:", time, ",data:[");
        for (DataPoint tuple : dataPointList) {
            sc.addTail(tuple);
        }
        sc.addTail("]}");
        return sc.toString();
    }
}
