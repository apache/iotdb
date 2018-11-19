package cn.edu.tsinghua.tsfile.timeseries.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.And;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.LtEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.NotEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.GtEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.LongInterval;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Eq;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Or;
import cn.edu.tsinghua.tsfile.timeseries.filter.verifier.LongFilterVerifier;

/**
 * 
 * @author CGF
 *
 */
public class FilterVerifierLongTest {
	
	private static String deltaObjectUID = FilterTestConstant.deltaObjectUID;
	private static String measurementUID = FilterTestConstant.measurementUID;
	
    @Test
    public void eqTest() {
        Eq<Long> eq = FilterFactory.eq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45L);
        LongInterval x = new LongFilterVerifier().getInterval(eq);
        assertEquals(x.count, 2);
        assertEquals(x.v[0], 45L);
        assertEquals(x.v[1], 45L);
    }
    
    @Test
    public void ltEqTest() {
        LtEq<Long> ltEq = FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45L, true);
        LongInterval x= new LongFilterVerifier().getInterval(ltEq);
        assertEquals(x.count, 2);
        assertEquals(x.v[0], Long.MIN_VALUE);
        assertEquals(x.v[1], 45L);
    }
    
    @Test
    public void andOrTest() {
        // [470,1200) & (500,800]|[1000,2000)
        
        GtEq<Long> gtEq1 = FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 470L, true);
        LtEq<Long> ltEq1 = FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1200L, false);
        And and1 = (And) FilterFactory.and(gtEq1, ltEq1);
        
        GtEq<Long> gtEq2 = FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500L, false);
        LtEq<Long> ltEq2 = FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800L, true);
        And and2 = (And) FilterFactory.and(gtEq2, ltEq2);
        
        GtEq<Long> gtEq3 = FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000L, true);
        LtEq<Long> ltEq3 = FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2000L, false);
        And and3 = (And) FilterFactory.and(gtEq3, ltEq3);
        Or or1 = (Or) FilterFactory.or(and2, and3);
        
        And andCombine1 = (And) FilterFactory.and(and1, or1);
        LongInterval ans = new LongFilterVerifier().getInterval(andCombine1);
        // ans.output();
        assertEquals(ans.count, 4);
        assertEquals(ans.v[0], 500L);
        assertEquals(ans.flag[0], false);
        assertEquals(ans.v[1], 800L);
        assertEquals(ans.flag[1], true);
        assertEquals(ans.v[2], 1000L);
        assertEquals(ans.flag[2], true);
        assertEquals(ans.v[3], 1200L);
        assertEquals(ans.flag[3], false);
        
        // for filter test coverage
        // [400, 500) (600, 800]
        GtEq<Long> gtEq4 = FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 400L, true);
        LtEq<Long> ltEq4 = FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500L, false);
        And and4 = (And) FilterFactory.and(gtEq4, ltEq4);
        
        GtEq<Long> gtEq5 = FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 600L, false);
        LtEq<Long> ltEq5 = FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800L, true);
        And and5 = (And) FilterFactory.and(gtEq5, ltEq5);
        
        And andNew = (And) FilterFactory.and(and4, and5);
        LongInterval ansNew = new LongFilterVerifier().getInterval(andNew);
        assertEquals(ansNew.count, 0);
        
        // for filter test coverage2
        // [600, 800] [400, 500] 
        GtEq<Long> gtEq6 = FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 600L, true);
        LtEq<Long> ltEq6 = FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800L, false);
        And and6 = (And) FilterFactory.and(gtEq6, ltEq6);
        
        GtEq<Long> gtEq7 = FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 400L, false);
        LtEq<Long> ltEq8 = FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500L, true);
        And and7 = (And) FilterFactory.and(gtEq7, ltEq8);
        
        And andCombine3 = (And) FilterFactory.and(and6, and7);
        LongInterval intervalAns = new LongFilterVerifier().getInterval(andCombine3);
        assertEquals(intervalAns.count, 0);
    }

    @Test
    public void andOrBorderTest() {

        // And Operator
        GtEq<Long> gtEq1 = FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2L, false);
        LtEq<Long> ltEq1 = FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2L, false);
        And and1 = (And) FilterFactory.and(gtEq1, ltEq1);
        LongInterval ans = new LongFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);
        and1 = (And) FilterFactory.and(ltEq1, gtEq1);
        ans = new LongFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);

        gtEq1 = FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2L, true);
        ltEq1 = FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2L, false);
        and1 = (And) FilterFactory.and(gtEq1, ltEq1);
        ans = new LongFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);
        and1 = (And) FilterFactory.and(ltEq1, gtEq1);
        ans = new LongFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);

        gtEq1 = FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2L, false);
        ltEq1 = FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2L, true);
        and1 = (And) FilterFactory.and(gtEq1, ltEq1);
        ans = new LongFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);
        and1 = (And) FilterFactory.and(ltEq1, gtEq1);
        ans = new LongFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);

        // Or Operator
        gtEq1 = FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2L, false);
        ltEq1 = FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2L, false);
        Or or1 = (Or) FilterFactory.or(gtEq1, ltEq1);
        ans = new LongFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 4);
        assertEquals(ans.v[0], Long.MIN_VALUE); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], 2L); assertEquals(ans.flag[1], false);
        assertEquals(ans.v[2], 2L); assertEquals(ans.flag[2], false);
        assertEquals(ans.v[3], Long.MAX_VALUE); assertEquals(ans.flag[3], true);
        or1 = (Or) FilterFactory.or(ltEq1, gtEq1);
        ans = new LongFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 4);
        assertEquals(ans.v[0], Long.MIN_VALUE); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], 2L); assertEquals(ans.flag[1], false);
        assertEquals(ans.v[2], 2L); assertEquals(ans.flag[2], false);
        assertEquals(ans.v[3], Long.MAX_VALUE); assertEquals(ans.flag[3], true);

        gtEq1 = FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2L, true);
        ltEq1 = FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2L, false);
        or1 = (Or) FilterFactory.or(gtEq1, ltEq1);
        ans = new LongFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 2);
        assertEquals(ans.v[0], Long.MIN_VALUE); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], Long.MAX_VALUE); assertEquals(ans.flag[1], true);
        or1 = (Or) FilterFactory.or(ltEq1, gtEq1);
        ans = new LongFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 2);
        assertEquals(ans.v[0], Long.MIN_VALUE); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], Long.MAX_VALUE); assertEquals(ans.flag[1], true);

        gtEq1 = FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2L, false);
        ltEq1 = FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2L, true);
        or1 = (Or) FilterFactory.or(gtEq1, ltEq1);
        ans = new LongFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 2);
        assertEquals(ans.v[0], Long.MIN_VALUE); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], Long.MAX_VALUE); assertEquals(ans.flag[1], true);
        or1 = (Or) FilterFactory.or(ltEq1, gtEq1);
        ans = new LongFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 2);
        assertEquals(ans.v[0], Long.MIN_VALUE); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], Long.MAX_VALUE); assertEquals(ans.flag[1], true);
    }

    @Test
    public void notEqTest() {
        NotEq<Long> notEq = FilterFactory.noteq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000L);
        LongInterval ans = new LongFilterVerifier().getInterval(notEq);
        
        assertEquals(ans.count, 4);
        assertEquals(ans.v[0], Long.MIN_VALUE);
        assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], 1000L);
        assertEquals(ans.flag[1], false);
        assertEquals(ans.v[2], 1000L);
        assertEquals(ans.flag[2], false);
        assertEquals(ans.v[3], Long.MAX_VALUE);
        assertEquals(ans.flag[3], true);
    }
    
    @Test
    public void orTest() {
        // [470,1200) | (500,800] | [1000,2000) | [100,200] 
        
        GtEq<Long> gtEq_11 = FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 470L, true);
        LtEq<Long> ltEq_11 = FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1200L, false);
        And and1 = (And) FilterFactory.and(gtEq_11, ltEq_11);
        
        GtEq<Long> gtEq_12 = FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500L, false);
        LtEq<Long> ltEq_12 = FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800L, true);
        And and2 = (And) FilterFactory.and(gtEq_12, ltEq_12);
        
        GtEq<Long> gtEq_13 = FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000L, true);
        LtEq<Long> ltEq_l3 = FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2000L, false);
        And and3 = (And) FilterFactory.and(gtEq_13, ltEq_l3);
        
        GtEq<Long> gtEq_14 = FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 100L, true);
        LtEq<Long> ltEq_14 = FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 200L, true);
        And and4 = (And) FilterFactory.and(gtEq_14, ltEq_14);
        
        Or o1 = (Or) FilterFactory.or(and2, and3);
        Or o2 = (Or) FilterFactory.or(o1, and4);
        
        Or or = (Or) FilterFactory.or(and1, o2);
        // LongInterval ans = (LongInterval) new LongFilterVerifier().getInterval(or);
        // System.out.println(ans);
        
        // answer may have overlap, but is right
        SingleValueVisitor<Long> vistor = new SingleValueVisitor<>(or);
        assertTrue(vistor.verify(500L));
        assertTrue(vistor.verify(600L));
        assertTrue(vistor.verify(1199L));
        assertTrue(vistor.verify(1999L));
        assertFalse(vistor.verify(5L));
        assertFalse(vistor.verify(2000L));
        assertFalse(vistor.verify(469L)); 
        assertFalse(vistor.verify(99L));
        assertTrue(vistor.verify(100L));
        assertTrue(vistor.verify(200L));
        assertFalse(vistor.verify(201L));
        
    }
    
}
