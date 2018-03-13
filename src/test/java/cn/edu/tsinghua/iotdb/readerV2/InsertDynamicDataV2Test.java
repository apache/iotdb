package cn.edu.tsinghua.iotdb.readerV2;

import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunk;
import cn.edu.tsinghua.iotdb.query.reader.InsertDynamicData;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowDeleteOperation;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperation;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperationReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowUpdateOperation;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeSortTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.OverflowInsertDataReader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory.*;

public class InsertDynamicDataV2Test {

    private String deltaObjectId = "device";
    private String measurementId = "sensor";
    private MeasurementDescriptor descriptor = new MeasurementDescriptor(measurementId, TSDataType.FLOAT, TSEncoding.RLE);
    private FilterSeries<Long> timeSeries = timeFilterSeries();
    private FilterSeries<Double> valueSeries = doubleFilterSeries(deltaObjectId, measurementId, FilterSeriesType.VALUE_FILTER);
    SingleSeriesFilterExpression timeFilter = ltEq(timeSeries, 197L, true);
    SingleSeriesFilterExpression valueFilter = gtEq(valueSeries, -333.0, false);

    @Test
    public void queryTest() throws IOException {
        InsertDynamicData reader = new InsertDynamicData(TSDataType.DOUBLE, timeFilter, valueFilter,
                buildFakedRawSeriesChunk(), buildFakedOverflowInsertDataReader(),
                buildFakedOverflowUpdateOperationReader());

        int cnt = 0;
        while (reader.hasNext()) {
            long time = reader.getCurrentMinTime();
            double value = reader.getCurrentDoubleValue();
            if (time == 75 || time == 95 || time == 115 || time == 120 || time == 125 || time == 145 || time == 150) {
                Assert.assertEquals(-time, value, 0.0);
            } else if (time >= 80 && time <= 90) {
                Assert.assertEquals(-111.0, value, 0.0);
            } else if (time >= 130 && time <= 140) {
                Assert.assertEquals(-222.0, value, 0.0);
            } else {
                Assert.assertEquals(time, value, 0.0);
            }
            cnt ++;
            //System.out.println(reader.getCurrentMinTime() + "," + reader.getCurrentDoubleValue());
            reader.removeCurrentValue();
        }
        //System.out.println(cnt);
        Assert.assertEquals(81, cnt);
    }

    public static class FakedRawSeriesChunk implements RawSeriesChunk {
        TSDataType dataType;
        long minTimestamp, maxTimestamp;
        TsPrimitiveType minValue, maxValue;
        Iterator<TimeValuePair> timeValuePairIterator;

        public FakedRawSeriesChunk(TSDataType dataType, long minTimestamp, long maxTimestamp, TsPrimitiveType minValue, TsPrimitiveType maxValue,
                                   Iterator<TimeValuePair> timeValuePairIterator) {
            this.dataType = dataType;
            this.minTimestamp = minTimestamp;
            this.maxTimestamp = maxTimestamp;
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.timeValuePairIterator = timeValuePairIterator;
        }

        @Override
        public TSDataType getDataType() {
            return dataType;
        }

        @Override
        public long getMaxTimestamp() {
            return maxTimestamp;
        }

        @Override
        public long getMinTimestamp() {
            return minTimestamp;
        }

        @Override
        public TsPrimitiveType getMaxValue() {
            return maxValue;
        }

        @Override
        public TsPrimitiveType getMinValue() {
            return minValue;
        }

        @Override
        public Iterator<TimeValuePair> getIterator() {
            return timeValuePairIterator;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }
    }

    public static class FakedOverflowInsertDataReader extends OverflowInsertDataReader {

        private List<TimeValuePair> timeValuePairList;
        private int index;

        public FakedOverflowInsertDataReader(List<TimeValuePair> timeValuePairList, Long jobId, PriorityMergeSortTimeValuePairReader seriesReader) {
            super(jobId, seriesReader);
            this.timeValuePairList = timeValuePairList;
        }

        @Override
        public boolean hasNext() throws IOException {
            return index < timeValuePairList.size();
        }

        @Override
        public TimeValuePair next() throws IOException {
            return timeValuePairList.get(index ++);
        }

        @Override
        public void skipCurrentTimeValuePair() throws IOException {
            next();
        }

        @Override
        public void close() throws IOException {
        }

        public TimeValuePair peek() throws IOException {
            return timeValuePairList.get(index);
        }
    }

    public static class FakedOverflowUpdateOperationReader implements OverflowOperationReader {

        List<OverflowOperation> overflowUpdateDataList;
        int index;

        public FakedOverflowUpdateOperationReader(List<OverflowOperation> overflowUpdateDataList) {
            this.overflowUpdateDataList = overflowUpdateDataList;
        }

        @Override
        public boolean hasNext() {
            return index < overflowUpdateDataList.size();
        }

        @Override
        public OverflowOperation next() {
            return overflowUpdateDataList.get(index ++);
        }

        @Override
        public OverflowOperation getCurrentOperation() {
            return overflowUpdateDataList.get(index);
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public OverflowOperationReader copy() {
            return null;
        }
    }

    private static FakedRawSeriesChunk buildFakedRawSeriesChunk() {
        List<TimeValuePair> list = new ArrayList<>();
        for (int i = 100;i <= 200;i ++) {
            list.add(new TimeValuePair(i, new TsPrimitiveType.TsDouble(i)));
        }

        return new FakedRawSeriesChunk(TSDataType.DOUBLE, 100, 200,
                new TsPrimitiveType.TsDouble(100.0), new TsPrimitiveType.TsDouble(200.0), list.iterator());
    }

    private static FakedOverflowInsertDataReader buildFakedOverflowInsertDataReader() {
        List<TimeValuePair> timeValuePairList = new ArrayList<>();
        for (int i = 50;i <= 150;i += 5) {
            timeValuePairList.add(new TimeValuePair(i, new TsPrimitiveType.TsDouble(-i)));
        }

        return new FakedOverflowInsertDataReader(timeValuePairList, 1L, null);
    }

    private static FakedOverflowUpdateOperationReader buildFakedOverflowUpdateOperationReader() {
        List<OverflowOperation> overflowUpdateDataList = new ArrayList<>();

        overflowUpdateDataList.add(new OverflowDeleteOperation(0L, 70L));
        overflowUpdateDataList.add(new OverflowUpdateOperation(80L, 90L, new TsPrimitiveType.TsDouble(-111.0)));
        overflowUpdateDataList.add(new OverflowDeleteOperation(100L, 110L));
        overflowUpdateDataList.add(new OverflowUpdateOperation(130L, 140L, new TsPrimitiveType.TsDouble(-222.0)));
        overflowUpdateDataList.add(new OverflowUpdateOperation(180L, 190L, new TsPrimitiveType.TsDouble(-333.0)));

        return new FakedOverflowUpdateOperationReader(overflowUpdateDataList);
    }
}
