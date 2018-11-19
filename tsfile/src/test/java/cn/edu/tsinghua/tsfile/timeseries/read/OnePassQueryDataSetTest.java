package cn.edu.tsinghua.tsfile.timeseries.read;


import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.query.OnePassQueryDataSet;
import org.junit.Assert;
import org.junit.Test;

public class OnePassQueryDataSetTest {

    @Test
    public void emptyQueryDataTest() {
        String[] ret = new String[]{
                "1	null	null",
                "2	2	null",
                "3	null	null",
                "4	4	null",
                "5	null	5.0",
                "6	6	6.0",
                "7	null	7.0",
                "8	8	8.0",
                "9	null	9.0",
                "10	10	10.0",
                "11	null	11.0",
                "12	null	12.0",
                "13	null	13.0",
                "14	null	14.0",
                "15	null	15.0",
                "16	null	16.0",
                "17	null	17.0",
                "18	null	18.0",
                "19	null	19.0",
                "20	null	20.0"
        };
        DynamicOneColumnData data1 = new DynamicOneColumnData(TSDataType.INT32, true, true);
        for (int i = 1; i <= 10; i++) {
            if (i % 2 == 0) {
                data1.putTime(i);
                data1.putInt(i);
            } else {
                data1.putEmptyTime(i);
            }
        }

        DynamicOneColumnData data2 = new DynamicOneColumnData(TSDataType.FLOAT, true, false);
        for (int i = 5; i <= 20; i++) {
            data2.putTime(i);
            data2.putFloat(i);
        }

        OnePassQueryDataSet onePassQueryDataSet = new OnePassQueryDataSet();
        onePassQueryDataSet.mapRet.put("d1.s1", data1);
        onePassQueryDataSet.mapRet.put("d1.s2", data2);

        int cnt = 0;
        while (onePassQueryDataSet.hasNextRecord()) {
            Assert.assertEquals(ret[cnt], onePassQueryDataSet.getNextRecord().toString());
            cnt ++;
            // System.out.println(onePassQueryDataSet.getNextRecord());
        }
    }
}
