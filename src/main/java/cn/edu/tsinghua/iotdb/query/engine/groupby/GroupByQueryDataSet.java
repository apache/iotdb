package cn.edu.tsinghua.iotdb.query.engine.groupby;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Field;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * QueryDataSet for group by aggregation.
 * Notice: this class is only used in test case.
 *
 * @author beyyes
 */
public class GroupByQueryDataSet extends QueryDataSet {

    private static final Logger logger = LoggerFactory.getLogger(GroupByQueryDataSet.class);

    private List<String> deltaObjectList, measurementList; // stores deltaObjectId, measurementId of each query
    private int querySize; // stores the number of group by query

    private int timeIndex = 0; // stores the read index of interval timestamps
    private List<Long> intervalTimestamps = new ArrayList<>(); // stores splitter timestamps of all interval
    private List<DynamicOneColumnData> validDataList = new ArrayList<>();  // stores valid value

    public GroupByQueryDataSet(List<String> deltaObjectList, List<String> measurementList) {
        logger.debug("Initialize GroupByQueryDataSet");
        this.deltaObjectList = deltaObjectList;
        this.measurementList = measurementList;
        this.querySize = deltaObjectList.size();
        testData();
    }

    public GroupByQueryDataSet() {
        testData();
    }

    /**
     * Group by aggregation is a special aggregation function,
     * all the query paths have the same result timestamps, eg:
     *
     * timestamp  s0   s1
     * 1          1.1  2
     * 3          4.4  null
     * 5          null 6
     *
     * @param intervalTimestamps all splitter timestamps, as shown above {1,3,5}
     * @param validDataList valid point which has value, as shown above s0:{1,3}, s1:{1,5}
     */
    public void putData(List<Long> intervalTimestamps, List<DynamicOneColumnData> validDataList) {
        this.intervalTimestamps = intervalTimestamps;
        this.validDataList = validDataList;
        this.timeIndex = 0;
    }

    private void testData() {
        intervalTimestamps.add(1L);
        intervalTimestamps.add(3L);
        intervalTimestamps.add(5L);
        deltaObjectList = new ArrayList<>();
        measurementList = new ArrayList<>();
        deltaObjectList.add("d0"); measurementList.add("s0");
        deltaObjectList.add("d0"); measurementList.add("s1");
        DynamicOneColumnData dataFloat = new DynamicOneColumnData(TSDataType.FLOAT, true);
        dataFloat.putTime(1L); dataFloat.putFloat(1.1f);
        dataFloat.putTime(3L); dataFloat.putFloat(4.4f);
        DynamicOneColumnData dataInt = new DynamicOneColumnData(TSDataType.INT32, true);
        dataInt.putTime(1L); dataInt.putInt(2);
        dataInt.putTime(5L); dataInt.putInt(6);
    }

    @Override
    public boolean hasNextRecord() {
        return timeIndex < intervalTimestamps.size();
    }

    @Override
    public RowRecord getNextRecord() {
        long minTime = intervalTimestamps.get(timeIndex);
        RowRecord rowRecord = new RowRecord(minTime, null, null);

        for (int i = 0; i < querySize; i++) {
            int validIndex = validDataList.get(i).curIdx;
            Field field = new Field(validDataList.get(i).dataType, deltaObjectList.get(i), measurementList.get(i));
            if (validIndex < validDataList.get(i).timeLength && minTime == validDataList.get(i).getTime(validIndex)) {
                field.setNull(false);
                putValueToField(validDataList.get(i), validIndex, field);
                validDataList.get(i).curIdx ++;
            } else {
                field.setNull(true);
            }
            rowRecord.addField(field);
        }

        timeIndex ++;
        return rowRecord;
    }
}
