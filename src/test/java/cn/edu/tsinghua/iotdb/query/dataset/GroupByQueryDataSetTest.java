package cn.edu.tsinghua.iotdb.query.dataset;

import cn.edu.tsinghua.iotdb.query.engine.groupby.GroupByQueryDataSet;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Junit test for <code>GroupByQueryDataSet</code>
 */
public class GroupByQueryDataSetTest {

    private List<String> deltaObjectList = new ArrayList<>();
    private List<String> measurementList = new ArrayList<>();
    private List<Long> intervalTimestamps = new ArrayList<>();

    @Test
    public void basicTest() {
        String[] retArray = new String[]{
                "1\t1.1\t2",
                "3\t4.4\tnull",
                "5\tnull\t6"
        };

        intervalTimestamps.add(1L);
        intervalTimestamps.add(3L);
        intervalTimestamps.add(5L);
        deltaObjectList.add("d0");
        measurementList.add("s0");
        deltaObjectList.add("d0");
        measurementList.add("s1");
        DynamicOneColumnData dataFloat = new DynamicOneColumnData(TSDataType.FLOAT, true);
        dataFloat.putTime(1L);
        dataFloat.putFloat(1.1f);
        dataFloat.putTime(3L);
        dataFloat.putFloat(4.4f);
        DynamicOneColumnData dataInt = new DynamicOneColumnData(TSDataType.INT32, true);
        dataInt.putTime(1L);
        dataInt.putInt(2);
        dataInt.putTime(5L);
        dataInt.putInt(6);
        List<DynamicOneColumnData> dynamicOneColumnDataList = new ArrayList<>();
        dynamicOneColumnDataList.add(dataFloat);
        dynamicOneColumnDataList.add(dataInt);

        GroupByQueryDataSet dataSet = new GroupByQueryDataSet(deltaObjectList, measurementList);
        dataSet.putData(intervalTimestamps, dynamicOneColumnDataList);
        int cnt = 0;
        while (dataSet.hasNextRecord()) {
            Assert.assertEquals(retArray[cnt], dataSet.getNextRecord().toString());
            cnt++;
            //System.out.println("=-" + dataSet.getNextRecord());
        }
    }
}
