package org.apache.iotdb.tsfile.read.filter;

import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.junit.Assert;
import org.junit.Test;

public class DigestFilterTest {

    private DigestForFilter digest1 = new DigestForFilter(1L, 100L, BytesUtils.intToBytes(1), BytesUtils.intToBytes(100), TSDataType.INT32);
    private DigestForFilter digest2 = new DigestForFilter(101L, 200L, BytesUtils.intToBytes(101), BytesUtils.intToBytes(200), TSDataType.INT32);

    @Test
    public void testEq() {
        Filter timeEq = TimeFilter.eq(10L);
        Assert.assertEquals(true, timeEq.satisfy(digest1));
        Assert.assertEquals(false, timeEq.satisfy(digest2));

        Filter valueEq = ValueFilter.eq(100);
        Assert.assertEquals(true, valueEq.satisfy(digest1));
        Assert.assertEquals(false, valueEq.satisfy(digest2));
    }

    @Test
    public void testGt() {
        Filter timeGt = TimeFilter.gt(100L);
        Assert.assertEquals(false, timeGt.satisfy(digest1));
        Assert.assertEquals(true, timeGt.satisfy(digest2));

        Filter valueGt = ValueFilter.gt(100);
        Assert.assertEquals(false, valueGt.satisfy(digest1));
        Assert.assertEquals(true, valueGt.satisfy(digest2));
    }

    @Test
    public void testGtEq() {
        Filter timeGtEq = TimeFilter.gtEq(100L);
        Assert.assertEquals(true, timeGtEq.satisfy(digest1));
        Assert.assertEquals(true, timeGtEq.satisfy(digest2));

        Filter valueGtEq = ValueFilter.gtEq(100);
        Assert.assertEquals(true, valueGtEq.satisfy(digest1));
        Assert.assertEquals(true, valueGtEq.satisfy(digest2));
    }

    @Test
    public void testLt() {
        Filter timeLt = TimeFilter.lt(101L);
        Assert.assertEquals(true, timeLt.satisfy(digest1));
        Assert.assertEquals(false, timeLt.satisfy(digest2));

        Filter valueLt = ValueFilter.lt(101);
        Assert.assertEquals(true, valueLt.satisfy(digest1));
        Assert.assertEquals(false, valueLt.satisfy(digest2));
    }

    @Test
    public void testLtEq() {
        Filter timeLtEq = TimeFilter.ltEq(101L);
        Assert.assertEquals(true, timeLtEq.satisfy(digest1));
        Assert.assertEquals(true, timeLtEq.satisfy(digest2));

        Filter valueLtEq = ValueFilter.ltEq(101);
        Assert.assertEquals(true, valueLtEq.satisfy(digest1));
        Assert.assertEquals(true, valueLtEq.satisfy(digest2));
    }

    @Test
    public void testAndOr() {
        Filter andFilter = FilterFactory.and(TimeFilter.gt(10L), ValueFilter.lt(50));
        Assert.assertEquals(true, andFilter.satisfy(digest1));
        Assert.assertEquals(false, andFilter.satisfy(digest2));

        Filter orFilter = FilterFactory.or(andFilter, TimeFilter.eq(200L));
        Assert.assertEquals(true, orFilter.satisfy(digest1));
        Assert.assertEquals(true, orFilter.satisfy(digest2));
    }

}
