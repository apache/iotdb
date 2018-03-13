package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.iotdb.utils.PrimitiveArrayList;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

/**
 * Created by zhangjinrui on 2018/1/26.
 */
public class PrimitiveMemSeriesLazyIterableTest {

    @Test
    public void test() {
        int count = 100;
        PrimitiveArrayList list = new PrimitiveArrayList(int.class);
        for (int i = 0; i < count; i++) {
            list.putTimestamp(i, i);
        }
        PrimitiveMemSeriesLazyIterable iterable = new PrimitiveMemSeriesLazyIterable(TSDataType.INT32, list);
        Iterator<TimeValuePair> it = iterable.iterator();
        int index = 0;
        while (it.hasNext()) {
            Assert.assertEquals(index, it.next().getTimestamp());
            index++;
        }
        Assert.assertEquals(count, index);
    }
}
