package cn.edu.tsinghua.tsfile.read.filter;

import cn.edu.tsinghua.tsfile.utils.Binary;
import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.read.filter.factory.FilterFactory;
import org.junit.Assert;
import org.junit.Test;


public class OperatorTest {
    private static final long EFFICIENCY_TEST_COUNT = 10000000;
    private static final long TESTED_TIMESTAMP = 1513585371L;

    @Test
    public void testEq() {
        Filter timeEq = TimeFilter.eq(100L);
        Assert.assertEquals(true, timeEq.satisfy(100, 100));
        Assert.assertEquals(false, timeEq.satisfy(101, 100));

        Filter filter2 = FilterFactory.and(TimeFilter.eq(100L), ValueFilter.eq(50));
        Assert.assertEquals(true, filter2.satisfy(100, 50));
        Assert.assertEquals(false, filter2.satisfy(100, 51));

        Filter filter3 = ValueFilter.eq(true);
        Assert.assertEquals(true, filter3.satisfy(100, true));
        Assert.assertEquals(false, filter3.satisfy(100, false));
    }

    @Test
    public void testGt() {
        Filter timeGt = TimeFilter.gt(TESTED_TIMESTAMP);
        Assert.assertEquals(true, timeGt.satisfy(TESTED_TIMESTAMP + 1, 100));
        Assert.assertEquals(false, timeGt.satisfy(TESTED_TIMESTAMP, 100));
        Assert.assertEquals(false, timeGt.satisfy(TESTED_TIMESTAMP - 1, 100));

        Filter valueGt = ValueFilter.gt(0.01f);
        Assert.assertEquals(true, valueGt.satisfy(TESTED_TIMESTAMP, 0.02f));
        Assert.assertEquals(false, valueGt.satisfy(TESTED_TIMESTAMP, 0.01f));
        Assert.assertEquals(false, valueGt.satisfy(TESTED_TIMESTAMP, -0.01f));

        Filter binaryFilter = ValueFilter.gt(new Binary("test1"));
        Assert.assertEquals(true, binaryFilter.satisfy(TESTED_TIMESTAMP, new Binary("test2")));
        Assert.assertEquals(false, binaryFilter.satisfy(TESTED_TIMESTAMP, new Binary("test0")));
    }

    @Test
    public void testGtEq() {
        Filter timeGtEq = TimeFilter.gtEq(TESTED_TIMESTAMP);
        Assert.assertEquals(true, timeGtEq.satisfy(
                TESTED_TIMESTAMP + 1, 100));
        Assert.assertEquals(true, timeGtEq.satisfy(
                TESTED_TIMESTAMP, 100));
        Assert.assertEquals(false, timeGtEq.satisfy(
                TESTED_TIMESTAMP - 1, 100));

        Filter valueGtEq = ValueFilter.gtEq(0.01);
        Assert.assertEquals(true, valueGtEq.satisfy(TESTED_TIMESTAMP, 0.02));
        Assert.assertEquals(true, valueGtEq.satisfy(TESTED_TIMESTAMP, 0.01));
        Assert.assertEquals(false, valueGtEq.satisfy(TESTED_TIMESTAMP, -0.01));
    }

    @Test
    public void testLt() {
        Filter timeLt = TimeFilter.lt(TESTED_TIMESTAMP);
        Assert.assertEquals(true, timeLt.satisfy(TESTED_TIMESTAMP - 1, 100));
        Assert.assertEquals(false, timeLt.satisfy(TESTED_TIMESTAMP, 100));
        Assert.assertEquals(false, timeLt.satisfy(TESTED_TIMESTAMP + 1, 100));

        Filter valueLt = ValueFilter.lt(100L);
        Assert.assertEquals(true, valueLt.satisfy(TESTED_TIMESTAMP, 99L));
        Assert.assertEquals(false, valueLt.satisfy(TESTED_TIMESTAMP, 100L));
        Assert.assertEquals(false, valueLt.satisfy(TESTED_TIMESTAMP, 101L));
    }

    @Test
    public void testLtEq() {
        Filter timeLtEq = TimeFilter.ltEq(TESTED_TIMESTAMP);
        Assert.assertEquals(true, timeLtEq.satisfy(TESTED_TIMESTAMP - 1, 100));
        Assert.assertEquals(true, timeLtEq.satisfy(TESTED_TIMESTAMP, 100));
        Assert.assertEquals(false, timeLtEq.satisfy(TESTED_TIMESTAMP + 1, 100));

        Filter valueLtEq = ValueFilter.ltEq(100L);
        Assert.assertEquals(true, valueLtEq.satisfy(TESTED_TIMESTAMP, 99L));
        Assert.assertEquals(true, valueLtEq.satisfy(TESTED_TIMESTAMP, 100L));
        Assert.assertEquals(false, valueLtEq.satisfy(TESTED_TIMESTAMP, 101L));
    }


    @Test
    public void testNot() {
        Filter timeLt = TimeFilter.not(TimeFilter.lt(TESTED_TIMESTAMP));
        Assert.assertEquals(false, timeLt.satisfy(TESTED_TIMESTAMP - 1, 100));
        Assert.assertEquals(true, timeLt.satisfy(TESTED_TIMESTAMP, 100));
        Assert.assertEquals(true, timeLt.satisfy(TESTED_TIMESTAMP + 1, 100));

        Filter valueLt = ValueFilter.not(ValueFilter.lt(100L));
        Assert.assertEquals(false, valueLt.satisfy(
                TESTED_TIMESTAMP, 99L));
        Assert.assertEquals(true, valueLt.satisfy(
                TESTED_TIMESTAMP, 100L));
        Assert.assertEquals(true, valueLt.satisfy(
                TESTED_TIMESTAMP, 101L));
    }

    @Test
    public void testNotEq() {
        Filter timeNotEq = TimeFilter.notEq(100L);
        Assert.assertEquals(false, timeNotEq.satisfy(
                100, 100));
        Assert.assertEquals(true, timeNotEq.satisfy(
                101, 100));

        Filter valueNotEq = ValueFilter.notEq(50);
        Assert.assertEquals(false, valueNotEq.satisfy(
                100, 50));
        Assert.assertEquals(true, valueNotEq.satisfy(
                100, 51));
    }

    @Test
    public void testAndOr() {
        Filter andFilter = FilterFactory.and(TimeFilter.gt(100L), ValueFilter.lt(50.9));
        Assert.assertEquals(true, andFilter.satisfy(101L, 50d));
        Assert.assertEquals(false, andFilter.satisfy(101L, 60d));
        Assert.assertEquals(false, andFilter.satisfy(99L, 50d));

        Filter orFilter = FilterFactory.or(andFilter, TimeFilter.eq(1000L));
        Assert.assertEquals(true, orFilter.satisfy(101L, 50d));
        Assert.assertEquals(false, orFilter.satisfy(101L, 60d));
        Assert.assertEquals(true, orFilter.satisfy(1000L, 50d));

        Filter andFilter2 = FilterFactory.and(orFilter, ValueFilter.notEq(50.0));
        Assert.assertEquals(false, andFilter2.satisfy(101L, 50d));
        Assert.assertEquals(false, andFilter2.satisfy(101L, 60d));
        Assert.assertEquals(true, andFilter2.satisfy(1000L, 51d));
    }

    @Test
    public void testWrongUsage() {
        Filter andFilter = FilterFactory.and(TimeFilter.gt(100L), ValueFilter.lt(true));
        try {
            andFilter.satisfy(101L, 50);
            Assert.fail();
        } catch (ClassCastException e) {

        }
    }

    @Test
    public void efficiencyTest() {
        Filter andFilter = FilterFactory.and(TimeFilter.gt(100L), ValueFilter.lt(50.9));
        Filter orFilter = FilterFactory.or(andFilter, TimeFilter.eq(1000L));

        long startTime = System.currentTimeMillis();
        for (long i = 0; i < EFFICIENCY_TEST_COUNT; i++) {
            orFilter.satisfy(i, i + 0.1);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("EfficiencyTest for Filter: \n\tFilter Expression = " + orFilter + "\n\tCOUNT = " + EFFICIENCY_TEST_COUNT +
                "\n\tTotal Time = " + (endTime - startTime) + "ms.");
    }
}
