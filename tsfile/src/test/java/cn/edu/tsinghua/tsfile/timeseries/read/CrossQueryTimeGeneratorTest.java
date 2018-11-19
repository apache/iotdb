package cn.edu.tsinghua.tsfile.timeseries.read;


import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.CrossSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.CrossQueryTimeGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class CrossQueryTimeGeneratorTest {

    private String d1 = "d1";
    private String s1 = "s1";
    private String s2 = "s2";
    private String s3 = "s3";
    private String s4 = "s4";


    @Test
    public void singleValueFilterSplitTest() throws IOException, ProcessorException {
        SingleSeriesFilterExpression timeFilter = FilterFactory.gtEq(FilterFactory.timeFilterSeries(), 60L, true);
        SingleSeriesFilterExpression s1Filter = FilterFactory.ltEq(FilterFactory.intFilterSeries(d1, s1, FilterSeriesType.VALUE_FILTER), 100, true);
        SingleSeriesFilterExpression s2Filter = FilterFactory.ltEq(FilterFactory.intFilterSeries(d1, s2, FilterSeriesType.VALUE_FILTER), 200, true);
        SingleSeriesFilterExpression s3Filter = FilterFactory.ltEq(FilterFactory.intFilterSeries(d1, s3, FilterSeriesType.VALUE_FILTER), 200, true);
        SingleSeriesFilterExpression s4Filter = FilterFactory.ltEq(FilterFactory.intFilterSeries(d1, s4, FilterSeriesType.VALUE_FILTER), 200, true);

        // [ (s1&s2) | s3 ] & s4
        CrossSeriesFilterExpression crossFilter = FilterFactory.csAnd(s1Filter, s2Filter);
        CrossSeriesFilterExpression crossFilter2 = FilterFactory.csOr(crossFilter, s3Filter);
        CrossSeriesFilterExpression crossFilter3 = FilterFactory.csAnd(crossFilter2, s4Filter);

        CrossQueryTimeGenerator generator = new CrossQueryTimeGenerator(timeFilter, null, crossFilter3, 1000) {
            @Override
            public DynamicOneColumnData getDataInNextBatch(DynamicOneColumnData res, int fetchSize, SingleSeriesFilterExpression valueFilter,
                                                           int valueFilterNumber) throws ProcessorException, IOException {
                DynamicOneColumnData data = new DynamicOneColumnData(TSDataType.INT32, true);
                //data.putTime(10L);
                //data.putInt(6);
                return data;
            }
        };

        long[] time = generator.generateTimes();
        Assert.assertEquals(time.length, 0);
        // System.out.println(time.length);
    }
}
