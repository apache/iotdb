package cn.edu.tsinghua.iotdb.query;

import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.page.PageWriterImpl;
import cn.edu.tsinghua.tsfile.timeseries.write.series.SeriesWriterImpl;
import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import static cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory.*;
import static org.junit.Assert.assertEquals;

/**
 * This class is a test for <code>InsertDynamicData</code>
 */
public class InsertDynamicDataTest {

    private String deltaObjectId = "device";
    private String measurementId = "sensor";
    private MeasurementDescriptor descriptor = new MeasurementDescriptor(measurementId, TSDataType.FLOAT, TSEncoding.RLE);
    private FilterSeries<Long> timeSeries = timeFilterSeries();
    private FilterSeries<Float> valueSeries = floatFilterSeries(deltaObjectId, measurementId, FilterSeriesType.VALUE_FILTER);

    @Test
    public void queryWithoutFilterTest() throws IOException {
        TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
        config.duplicateIncompletedPage = true;
        SeriesWriterImpl writer = new SeriesWriterImpl(deltaObjectId, descriptor, new PageWriterImpl(descriptor), 1000);
        for (long i = 100;i <= 500; i++) {
            writer.write(i, (float) i - 50);
        }
        for (long i = 700;i <= 1000; i++) {
            writer.write(i, (float) i);
        }

        List<Object> writeList = writer.query();
        Pair<List<ByteArrayInputStream>, CompressionTypeName> pair = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) writeList.get(1);
        List<ByteArrayInputStream> sealedPageList = pair.left;
        CompressionTypeName compressionTypeName = pair.right;
        DynamicOneColumnData lastPageData = (DynamicOneColumnData) writeList.get(0);
        DynamicOneColumnData overflowInsert = buildOverflowInsertData();
        DynamicOneColumnData overflowUpdateTrue = buildOverflowUpdateTrue();
        DynamicOneColumnData overflowUpdateFalse = buildOverflowUpdateFalse();

        InsertDynamicData insertDynamicData = new InsertDynamicData(TSDataType.FLOAT, compressionTypeName, sealedPageList, lastPageData,
                overflowInsert, overflowUpdateTrue, overflowUpdateFalse, null, null);

        int cnt = 0;
        while (insertDynamicData.hasInsertData()) {
            long time = insertDynamicData.getCurrentMinTime();
            float value = insertDynamicData.getCurrentFloatValue();
            if (time >= 50 && time <= 60) {
                assertEquals(value, -111, 0.0);
            } else if (time >= 100 && time <= 520) {
                if (time >= 200 && time <= 220) {
                    assertEquals( -111, value, 0.0);
                } else if (time >= 490 && time < 510) {
                    assertEquals(-222, value,0.0);
                } else if (time >= 510 && time <= 520) {
                    assertEquals(-111, value, 0.0);
                } else {
                    assertEquals(time-50, value, 0.0);
                }
            } else {
                if (time >= 900 && time <= 910) {
                    assertEquals(value, -111, 0.0);
                } else if (time >= 960 && time <= 965) {
                    assertEquals(value, -222, 0.0);
                } else {
                    assertEquals(value, time, 0.0);
                }
            }
            // System.out.println(time + "," + value);
            cnt ++;
            insertDynamicData.removeCurrentValue();
            //System.out.println(time + "," + value);
        }
        assertEquals(670, cnt);
        //System.out.println("..." + cnt);
    }

    @Test
    public void queryFilterTest() throws IOException {
        TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
        config.duplicateIncompletedPage = true;
        SeriesWriterImpl writer = new SeriesWriterImpl(deltaObjectId, descriptor, new PageWriterImpl(descriptor), 1000);
        for (long i = 100;i <= 500; i++) {
            writer.write(i, (float)i - 50);
        }
        for (long i = 700;i <= 1000; i++) {
            writer.write(i, (float)i);
        }

        List<Object> writeList = writer.query();
        Pair<List<ByteArrayInputStream>, CompressionTypeName> pair = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) writeList.get(1);
        List<ByteArrayInputStream> sealedPageList = pair.left;
        CompressionTypeName compressionTypeName = pair.right;
        DynamicOneColumnData lastPageData = (DynamicOneColumnData) writeList.get(0);
        DynamicOneColumnData overflowInsert = buildOverflowInsertData();
        DynamicOneColumnData overflowUpdateTrue = buildOverflowUpdateTrue();
        DynamicOneColumnData overflowUpdateFalse = buildOverflowUpdateFalse();

        SingleSeriesFilterExpression timeFilter = ltEq(timeSeries, 560L, true);
        SingleSeriesFilterExpression valueFilter = gtEq(valueSeries, -300.0f, true);
        InsertDynamicData insertDynamicData = new InsertDynamicData(TSDataType.FLOAT, compressionTypeName, sealedPageList, lastPageData,
                overflowInsert, overflowUpdateTrue, overflowUpdateFalse, timeFilter, valueFilter);
        while (insertDynamicData.hasInsertData()) {
            long time = insertDynamicData.getCurrentMinTime();
            float value = insertDynamicData.getCurrentFloatValue();
            System.out.println(time + "," + value);
            insertDynamicData.removeCurrentValue();
        }
    }

    private DynamicOneColumnData buildOverflowInsertData() {
        // -111 : insert operation
        DynamicOneColumnData overflowInsert = new DynamicOneColumnData(TSDataType.FLOAT, true);
        for (int i = 50;i <= 60;i ++) {
            overflowInsert.putTime(i);
            overflowInsert.putFloat(-111);
        }

        for (int i = 200;i <= 220;i++) {
            overflowInsert.putTime(i);
            overflowInsert.putFloat(-111);
        }

        for (int i = 510;i <= 520;i++) {
            overflowInsert.putTime(i);
            overflowInsert.putFloat(-111);
        }

        for (int i = 900;i <= 910;i++) {
            overflowInsert.putTime(i);
            overflowInsert.putFloat(-111);
        }
        return overflowInsert;
    }

    private DynamicOneColumnData buildOverflowUpdateTrue() {
        DynamicOneColumnData ans = new DynamicOneColumnData(TSDataType.FLOAT, true);
        ans.putTime(490);
        ans.putTime(500);
        ans.putFloat(-222.0f);

        ans.putTime(960);
        ans.putTime(965);
        ans.putFloat(-222.0f);
        return ans;
    }

    private DynamicOneColumnData buildOverflowUpdateFalse() {
        DynamicOneColumnData ans = new DynamicOneColumnData(TSDataType.FLOAT, true);
        ans.putTime(300);
        ans.putTime(350);
        ans.putFloat(-333.0f);

        ans.putTime(980);
        ans.putTime(990);
        ans.putFloat(-333.0f);
        return ans;
    }
}
