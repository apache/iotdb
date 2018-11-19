package cn.edu.tsinghua.tsfile.timeseries.filterV2;

import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.impl.TimeValuePairFilterVisitorImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by zhangjinrui on 2017/12/18.
 */
public class OperatorTest {
    private static final long EFFICIENCY_TEST_COUNT = 10000000;
    private static final long TESTED_TIMESTAMP = 1513585371L;
    private TimeValuePairFilterVisitorImpl timeValuePairFilterVisitor = new TimeValuePairFilterVisitorImpl();

    @Test
    public void testEq() {
        Filter<Long> timeEq = TimeFilter.eq(100L);
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(100, new TsPrimitiveType.TsInt(100)), timeEq));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(101, new TsPrimitiveType.TsInt(100)), timeEq));

        Filter<Integer> filter2 = FilterFactory.and(TimeFilter.eq(100L), ValueFilter.eq(50));
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(100, new TsPrimitiveType.TsInt(50)), filter2));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(100, new TsPrimitiveType.TsInt(51)), filter2));

        Filter<Boolean> filter3 = ValueFilter.eq(true);
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(100, new TsPrimitiveType.TsBoolean(true)), filter3));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(100, new TsPrimitiveType.TsBoolean(false)), filter3));
    }

    @Test
    public void testGt() {
        Filter timeGt = TimeFilter.gt(TESTED_TIMESTAMP);
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP + 1, new TsPrimitiveType.TsInt(100)), timeGt));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsInt(100)), timeGt));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP - 1, new TsPrimitiveType.TsInt(100)), timeGt));

        Filter<Float> valueGt = ValueFilter.gt(0.01f);
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsFloat(0.02f)), valueGt));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsFloat(0.01f)), valueGt));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsFloat(-0.01f)), valueGt));

        Filter<Binary> binaryFilter = ValueFilter.gt(new Binary("test1"));
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsBinary(new Binary("test2"))), binaryFilter));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsBinary(new Binary("test0"))), binaryFilter));
    }

    @Test
    public void testGtEq() {
        Filter timeGtEq = TimeFilter.gtEq(TESTED_TIMESTAMP);
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP + 1, new TsPrimitiveType.TsInt(100)), timeGtEq));
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsInt(100)), timeGtEq));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP - 1, new TsPrimitiveType.TsInt(100)), timeGtEq));

        Filter<Double> valueGtEq = ValueFilter.gtEq(0.01);
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsDouble(0.02)), valueGtEq));
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsDouble(0.01)), valueGtEq));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsDouble(-0.01)), valueGtEq));
    }

    @Test
    public void testLt() {
        Filter timeLt = TimeFilter.lt(TESTED_TIMESTAMP);
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP - 1, new TsPrimitiveType.TsInt(100)), timeLt));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsInt(100)), timeLt));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP + 1, new TsPrimitiveType.TsInt(100)), timeLt));

        Filter<Long> valueLt = ValueFilter.lt(100L);
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsLong(99L)), valueLt));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsLong(100L)), valueLt));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsLong(101L)), valueLt));
    }

    @Test
    public void testLtEq() {
        Filter timeLtEq = TimeFilter.ltEq(TESTED_TIMESTAMP);
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP - 1, new TsPrimitiveType.TsInt(100)), timeLtEq));
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsInt(100)), timeLtEq));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP + 1, new TsPrimitiveType.TsInt(100)), timeLtEq));

        Filter<Long> valueLtEq = ValueFilter.ltEq(100L);
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsLong(99L)), valueLtEq));
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsLong(100L)), valueLtEq));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsLong(101L)), valueLtEq));
    }

    @Test
    public void testNoRestriction() {
        Filter timeNoRestriction = TimeFilter.noRestriction();
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP - 1, new TsPrimitiveType.TsInt(100)), timeNoRestriction));

        Filter<Long> valueNoRestriction = ValueFilter.noRestriction();
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP - 1, new TsPrimitiveType.TsLong(100L)), valueNoRestriction));
    }

    @Test
    public void testNot() {
        Filter timeLt = TimeFilter.not(TimeFilter.lt(TESTED_TIMESTAMP));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP - 1, new TsPrimitiveType.TsInt(100)), timeLt));
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsInt(100)), timeLt));
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP + 1, new TsPrimitiveType.TsInt(100)), timeLt));

        Filter<Long> valueLt = ValueFilter.not(ValueFilter.lt(100L));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsLong(99L)), valueLt));
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsLong(100L)), valueLt));
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsLong(101L)), valueLt));
    }

    @Test
    public void testNotEq() {
        Filter<Long> timeNotEq = TimeFilter.notEq(100L);
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(100, new TsPrimitiveType.TsInt(100)), timeNotEq));
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(101, new TsPrimitiveType.TsInt(100)), timeNotEq));

        Filter<Integer> valueNotEq = ValueFilter.notEq(50);
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(100, new TsPrimitiveType.TsInt(50)), valueNotEq));
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(100, new TsPrimitiveType.TsInt(51)), valueNotEq));
    }

    @Test
    public void testAndOr() {
        Filter<Double> andFilter = FilterFactory.and(TimeFilter.gt(100L), ValueFilter.lt(50.9));
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(101L, new TsPrimitiveType.TsDouble(50)), andFilter));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(101L, new TsPrimitiveType.TsDouble(60)), andFilter));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(99L, new TsPrimitiveType.TsDouble(50)), andFilter));

        Filter<Double> orFilter = FilterFactory.or(andFilter, TimeFilter.eq(1000L));
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(101L, new TsPrimitiveType.TsDouble(50)), orFilter));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(101L, new TsPrimitiveType.TsDouble(60)), orFilter));
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(1000L, new TsPrimitiveType.TsDouble(50)), orFilter));

        Filter<Double> andFilter2 = FilterFactory.and(orFilter, ValueFilter.notEq(50.0));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(101L, new TsPrimitiveType.TsDouble(50)), andFilter2));
        Assert.assertEquals(false, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(101L, new TsPrimitiveType.TsDouble(60)), andFilter2));
        Assert.assertEquals(true, timeValuePairFilterVisitor.satisfy(
                new TimeValuePair(1000L, new TsPrimitiveType.TsDouble(51)), andFilter2));
    }

    @Test
    public void testWrongUsage() {
        Filter<Boolean> andFilter = FilterFactory.and(TimeFilter.gt(100L), ValueFilter.lt(true));
        TimeValuePair timeValuePair = new TimeValuePair(101L, new TsPrimitiveType.TsLong(50));
        try {
            timeValuePairFilterVisitor.satisfy(timeValuePair, andFilter);
            Assert.fail();
        }catch (ClassCastException e){
            
        }
    }

    @Test
    public void efficiencyTest() {
        Filter<Double> andFilter = FilterFactory.and(TimeFilter.gt(100L), ValueFilter.lt(50.9));
        Filter<Double> orFilter = FilterFactory.or(andFilter, TimeFilter.eq(1000L));

        long startTime = System.currentTimeMillis();
        for (long i = 0; i < EFFICIENCY_TEST_COUNT; i++) {
            TimeValuePair tvPair = new TimeValuePair(Long.valueOf(i), new TsPrimitiveType.TsDouble(i + 0.1));
            timeValuePairFilterVisitor.satisfy(tvPair, orFilter);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("EfficiencyTest for Filter: \n\tFilter Expression = " + orFilter + "\n\tCOUNT = " + EFFICIENCY_TEST_COUNT +
                "\n\tTotal Time = " + (endTime - startTime) + "ms.");
    }
}
