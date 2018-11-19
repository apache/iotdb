package cn.edu.tsinghua.tsfile.timeseries.filterV2;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.exception.QueryFilterOptimizationException;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.QueryFilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.util.QueryFilterOptimizer;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.util.QueryFilterPrinter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/19.
 */
public class QueryFilterOptimizerTest {

    private QueryFilterOptimizer queryFilterOptimizer = QueryFilterOptimizer.getInstance();
    private List<Path> selectedSeries;

    @Before
    public void before() {
        selectedSeries = new ArrayList<>();
        selectedSeries.add(new Path("d1.s1"));
        selectedSeries.add(new Path("d2.s1"));
        selectedSeries.add(new Path("d1.s2"));
        selectedSeries.add(new Path("d1.s2"));
    }

    @After
    public void after() {
        selectedSeries.clear();
    }

    @Test
    public void testTimeOnly() {
        try {
            Filter timeFilter = TimeFilter.lt(100L);
            QueryFilter queryFilter = new GlobalTimeFilter(timeFilter);
            System.out.println(queryFilterOptimizer.convertGlobalTimeFilter(queryFilter, selectedSeries));

            QueryFilter queryFilter2 = QueryFilterFactory.or(
                    QueryFilterFactory.and(new GlobalTimeFilter(TimeFilter.lt(50L)), new GlobalTimeFilter(TimeFilter.gt(10L))),
                    new GlobalTimeFilter(TimeFilter.gt(200L)));
            QueryFilterPrinter.print(queryFilterOptimizer.convertGlobalTimeFilter(queryFilter2, selectedSeries));

        } catch (QueryFilterOptimizationException e) {
            e.printStackTrace();
        }


    }

    @Test
    public void testSeriesOnly() {
        try {
            Filter<Long> filter1 = FilterFactory.and(FilterFactory.or(
                    ValueFilter.gt(100L), ValueFilter.lt(50L)), TimeFilter.gt(1400L));
            SeriesFilter<Long> seriesFilter1 = new SeriesFilter<>(new Path("d2.s1"), filter1);

            Filter<Float> filter2 = FilterFactory.and(FilterFactory.or(
                    ValueFilter.gt(100.5f), ValueFilter.lt(50.6f)), TimeFilter.gt(1400L));
            SeriesFilter<Float> seriesFilter2 = new SeriesFilter<>(new Path("d1.s2"), filter2);

            Filter<Double> filter3 = FilterFactory.or(FilterFactory.or(
                    ValueFilter.gt(100.5), ValueFilter.lt(50.6)), TimeFilter.gt(1400L));
            SeriesFilter<Double> seriesFilter3 = new SeriesFilter<>(new Path("d2.s2"), filter3);

            QueryFilter queryFilter = QueryFilterFactory.and(QueryFilterFactory.or(seriesFilter1, seriesFilter2), seriesFilter3);
            Assert.assertEquals(true, queryFilter.toString().equals(
                    queryFilterOptimizer.convertGlobalTimeFilter(queryFilter, selectedSeries).toString()));

        } catch (QueryFilterOptimizationException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testOneTimeAndSeries() {
        Filter<Long> filter1 = FilterFactory.or(ValueFilter.gt(100L), ValueFilter.lt(50L));
        SeriesFilter<Long> seriesFilter1 = new SeriesFilter<>(new Path("d2.s1"), filter1);

        Filter<Float> filter2 = FilterFactory.or(ValueFilter.gt(100.5f), ValueFilter.lt(50.6f));
        SeriesFilter<Float> seriesFilter2 = new SeriesFilter<>(new Path("d1.s2"), filter2);

        Filter<Double> filter3 = FilterFactory.or(ValueFilter.gt(100.5), ValueFilter.lt(50.6));
        SeriesFilter<Double> seriesFilter3 = new SeriesFilter<>(new Path("d2.s2"), filter3);

        Filter timeFilter = TimeFilter.lt(14001234L);
        QueryFilter globalTimeFilter = new GlobalTimeFilter(timeFilter);
        QueryFilter queryFilter = QueryFilterFactory.and(QueryFilterFactory.or(seriesFilter1, seriesFilter2), globalTimeFilter);
        QueryFilterPrinter.print(queryFilter);
        try {
            String rightRet = "[[d2.s1:((value > 100 || value < 50) && time < 14001234)] || [d1.s2:((value > 100.5 || value < 50.6) && time < 14001234)]]";
            QueryFilter regularFilter = queryFilterOptimizer.convertGlobalTimeFilter(queryFilter, selectedSeries);
            Assert.assertEquals(true, rightRet.equals(regularFilter.toString()));
            QueryFilterPrinter.print(regularFilter);
        } catch (QueryFilterOptimizationException e) {
            Assert.fail();
        }
    }

    @Test
    public void testOneTimeOrSeries() {
        Filter<Long> filter1 = FilterFactory.or(ValueFilter.gt(100L), ValueFilter.lt(50L));
        SeriesFilter<Long> seriesFilter1 = new SeriesFilter<>(
                new Path("d2.s1"), filter1);

        Filter<Float> filter2 = FilterFactory.or(ValueFilter.gt(100.5f), ValueFilter.lt(50.6f));
        SeriesFilter<Float> seriesFilter2 = new SeriesFilter<>(
                new Path("d1.s2"), filter2);

        Filter<Double> filter3 = FilterFactory.or(ValueFilter.gt(100.5), ValueFilter.lt(50.6));
        SeriesFilter<Double> seriesFilter3 = new SeriesFilter<>(
                new Path("d2.s2"), filter3);
        Filter timeFilter = TimeFilter.lt(14001234L);
        QueryFilter globalTimeFilter = new GlobalTimeFilter(timeFilter);
        QueryFilter queryFilter = QueryFilterFactory.or(QueryFilterFactory.or(seriesFilter1, seriesFilter2), globalTimeFilter);
        QueryFilterPrinter.print(queryFilter);

        try {
            String rightRet = "[[[[[d1.s1:time < 14001234] || [d2.s1:time < 14001234]] || [d1.s2:time < 14001234]] || " +
                    "[d1.s2:time < 14001234]] || [[d2.s1:(value > 100 || value < 50)] || [d1.s2:(value > 100.5 || value < 50.6)]]]";
            QueryFilter regularFilter = queryFilterOptimizer.convertGlobalTimeFilter(queryFilter, selectedSeries);
            Assert.assertEquals(true, rightRet.equals(regularFilter.toString()));
            QueryFilterPrinter.print(regularFilter);
        } catch (QueryFilterOptimizationException e) {
            Assert.fail();
        }
    }

    @Test
    public void testTwoTimeCombine() {
        Filter<Long> filter1 = FilterFactory.or(ValueFilter.gt(100L), ValueFilter.lt(50L));
        SeriesFilter<Long> seriesFilter1 = new SeriesFilter<>(new Path("d2.s1"), filter1);

        Filter<Float> filter2 = FilterFactory.or(ValueFilter.gt(100.5f), ValueFilter.lt(50.6f));
        SeriesFilter<Float> seriesFilter2 = new SeriesFilter<>(new Path("d1.s2"), filter2);

        Filter<Double> filter3 = FilterFactory.or(ValueFilter.gt(100.5), ValueFilter.lt(50.6));
        SeriesFilter<Double> seriesFilter3 = new SeriesFilter<>(new Path("d2.s2"), filter3);

        QueryFilter globalTimeFilter1 = new GlobalTimeFilter(TimeFilter.lt(14001234L));
        QueryFilter globalTimeFilter2 = new GlobalTimeFilter(TimeFilter.gt(14001000L));
        QueryFilter queryFilter = QueryFilterFactory.or(QueryFilterFactory.or(seriesFilter1, seriesFilter2),
                QueryFilterFactory.and(globalTimeFilter1, globalTimeFilter2));

        try {
            String rightRet = "[[[[[d1.s1:(time < 14001234 && time > 14001000)] || [d2.s1:(time < 14001234 && time > 14001000)]] " +
                    "|| [d1.s2:(time < 14001234 && time > 14001000)]] || [d1.s2:(time < 14001234 && time > 14001000)]] " +
                    "|| [[d2.s1:(value > 100 || value < 50)] || [d1.s2:(value > 100.5 || value < 50.6)]]]";
            QueryFilter regularFilter = queryFilterOptimizer.convertGlobalTimeFilter(queryFilter, selectedSeries);
            Assert.assertEquals(true, rightRet.equals(regularFilter.toString()));
        } catch (QueryFilterOptimizationException e) {
            Assert.fail();
        }

        QueryFilter queryFilter2 = QueryFilterFactory.and(QueryFilterFactory.or(seriesFilter1, seriesFilter2),
                QueryFilterFactory.and(globalTimeFilter1, globalTimeFilter2));

        try {
            String rightRet2 = "[[d2.s1:((value > 100 || value < 50) && (time < 14001234 && time > 14001000))] || " +
                    "[d1.s2:((value > 100.5 || value < 50.6) && (time < 14001234 && time > 14001000))]]";
            QueryFilter regularFilter2 = queryFilterOptimizer.convertGlobalTimeFilter(queryFilter2, selectedSeries);
            Assert.assertEquals(true, rightRet2.equals(regularFilter2.toString()));
        } catch (QueryFilterOptimizationException e) {
            Assert.fail();
        }

        QueryFilter queryFilter3 = QueryFilterFactory.or(queryFilter2, queryFilter);
        QueryFilterPrinter.print(queryFilter3);
        try {
            QueryFilter regularFilter3 = queryFilterOptimizer.convertGlobalTimeFilter(queryFilter3, selectedSeries);
            QueryFilterPrinter.print(regularFilter3);
        } catch (QueryFilterOptimizationException e) {
            Assert.fail();
        }
    }
}