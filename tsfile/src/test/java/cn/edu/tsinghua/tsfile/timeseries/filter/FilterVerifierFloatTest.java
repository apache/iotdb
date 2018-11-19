package cn.edu.tsinghua.tsfile.timeseries.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.And;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.LtEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.NotEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.FloatInterval;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.GtEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Or;
import cn.edu.tsinghua.tsfile.timeseries.filter.verifier.FloatFilterVerifier;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Eq;

public class FilterVerifierFloatTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(FilterVerifierFloatTest.class);
    
    private static final float float_min_delta = 0.00001f;
	private static String deltaObjectUID = FilterTestConstant.deltaObjectUID;
	private static String measurementUID = FilterTestConstant.measurementUID;
	
    @Test
    public void eqTest() {
        Eq<Float> eq = FilterFactory.eq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45.0f);
        FloatInterval x = (FloatInterval) new FloatFilterVerifier().getInterval(eq);
        assertEquals(x.count, 2);
        assertEquals(x.v[0], 45, float_min_delta);
        assertEquals(x.v[1], 45, float_min_delta);
    }
    
    @Test
    public void ltEqTest() {
        LtEq<Float> ltEq = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45.0f, true);
        FloatInterval x = (FloatInterval) new FloatFilterVerifier().getInterval(ltEq);
        assertEquals(x.count, 2);
        assertEquals(x.v[0], -Float.MAX_VALUE, float_min_delta);
        assertEquals(x.v[1], 45.0f, float_min_delta);

        ltEq = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), -45.0f, true);
        SingleValueVisitor<Float> visitor = new SingleValueVisitor<>(ltEq);
        Assert.assertTrue(visitor.verify(-46.0f));
        Assert.assertFalse(visitor.verify(-40.0f));
        Assert.assertFalse(visitor.verify(70.0f));
    }
    
    @Test
    public void andOrTest() {
        // [470,1200) & (500,800]|[1000,2000)
        
        GtEq<Float> gtEq1 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 470.0f, true);
        LtEq<Float> ltEq1 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1200.0f, false);
        And and1 = (And) FilterFactory.and(gtEq1, ltEq1);
        
        GtEq<Float> gtEq2 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500.0f, false);
        LtEq<Float> ltEq2 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800.0f, true);
        And and2 = (And) FilterFactory.and(gtEq2, ltEq2);
        
        GtEq<Float> gtEq3 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000.0f, true);
        LtEq<Float> ltEq3 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2000.0f, false);
        And and3 = (And) FilterFactory.and(gtEq3, ltEq3);
        Or or1 = (Or) FilterFactory.or(and2, and3);
        
        And andCombine1 = (And) FilterFactory.and(and1, or1);
        FloatInterval ans = (FloatInterval) new FloatFilterVerifier().getInterval(andCombine1);
        
        LOG.info("and+Or Test");
        assertEquals(ans.count, 4);
        assertEquals(ans.v[0], 500.0f, float_min_delta); assertEquals(ans.flag[0], false);
        assertEquals(ans.v[1], 800.0f, float_min_delta); assertEquals(ans.flag[1], true);
        assertEquals(ans.v[2], 1000.0f, float_min_delta); assertEquals(ans.flag[2], true); 
        assertEquals(ans.v[3], 1200.0f, float_min_delta); assertEquals(ans.flag[3], false);
        
        
        // for filter test coverage
        // [400, 500) (600, 800]
        GtEq<Float> gtEq4 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 400.0f, true);
        LtEq<Float> ltEq4 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500.0f, false);
        And and4 = (And) FilterFactory.and(gtEq4, ltEq4);
        
        GtEq<Float> gtEq5 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 600.0f, false);
        LtEq<Float> ltEq5 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800.0f, true);
        And and5 = (And) FilterFactory.and(gtEq5, ltEq5);
        
        And andNew = (And) FilterFactory.and(and4, and5);
        FloatInterval ansNew = (FloatInterval) new FloatFilterVerifier().getInterval(andNew);
        assertEquals(ansNew.count, 0);
         
        // for filter test coverage2
        // [600, 800] [400, 500] 
        GtEq<Float> gtEq6 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 600.0f, true);
        LtEq<Float> ltEq6 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800.0f, false);
        And and6 = (And) FilterFactory.and(gtEq6, ltEq6);
        
        GtEq<Float> gtEq7 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 400.0f, false);
        LtEq<Float> ltEq8 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500.0f, true);
        And and7 = (And) FilterFactory.and(gtEq7, ltEq8);
        
        And andCombine3 = (And) FilterFactory.and(and6, and7);
        FloatInterval intervalAns = (FloatInterval) new FloatFilterVerifier().getInterval(andCombine3);
        assertEquals(intervalAns.count, 0);
    }
    
    @Test
    public void notEqTest() {
        NotEq<Float> notEq = FilterFactory.noteq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000.0f);
        FloatInterval ans = (FloatInterval) new FloatFilterVerifier().getInterval(notEq);
        
        assertEquals(ans.count, 4);
        assertEquals(ans.v[0], -Float.MAX_VALUE, float_min_delta); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], 1000.0f, float_min_delta); assertEquals(ans.flag[1], false);
        assertEquals(ans.v[2], 1000.0f, float_min_delta); assertEquals(ans.flag[2], false);
        assertEquals(ans.v[3], Float.MAX_VALUE, float_min_delta); assertEquals(ans.flag[3], true);
    }
    
    @Test
    public void orTest() {
        // [470,1200) | (500,800] | [1000,2000) | [100,200]
        
        GtEq<Float> gtEq_11 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 470.0f, true);
        LtEq<Float> ltEq_11 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1200.0f, false);
        And and1 = (And) FilterFactory.and(gtEq_11, ltEq_11);
        
        GtEq<Float> gtEq_12 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500.0f, false);
        LtEq<Float> ltEq_12 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800.0f, true);
        And and2 = (And) FilterFactory.and(gtEq_12, ltEq_12);
        
        GtEq<Float> gtEq_13 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000.0f, true);
        LtEq<Float> ltEq_l3 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2000.0f, false);
        And and3 = (And) FilterFactory.and(gtEq_13, ltEq_l3);
        
        GtEq<Float> gtEq_14 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 100.0f, true);
        LtEq<Float> ltEq_14 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 200.0f, true);
        And and4 = (And) FilterFactory.and(gtEq_14, ltEq_14);
        
        Or or1 = (Or) FilterFactory.or(and2, and3);
        Or or2 = (Or) FilterFactory.or(or1, and4);
        
        Or orCombine = (Or) FilterFactory.or(and1, or2);
        FloatInterval ans = (FloatInterval) new FloatFilterVerifier().getInterval(orCombine);
        // System.out.println(ans);
        // LOG.info("or Test output");
        
        // answer may have overlap, but is right
        SingleValueVisitor<Float> vistor = new SingleValueVisitor<>(orCombine);
        assertTrue(vistor.verify(500.0f));
        assertTrue(vistor.verify(600.0f));
        assertTrue(vistor.verify(1199.0f)); 
        assertTrue(vistor.verify(1999.0f));
        assertFalse(vistor.verify(5.0f));
        assertFalse(vistor.verify(2000.0f));
        assertFalse(vistor.verify(469.0f));
        assertFalse(vistor.verify(99.0f));
        assertTrue(vistor.verify(100.0f));
        assertTrue(vistor.verify(200.0f));
        assertFalse(vistor.verify(201.0f)); 
        
    }
    
    @Test
    public void orborderTest() {
        // [470,1200] | [1200, 1500]
        
        GtEq<Float> gtEq_11 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 470.0f, true);
        LtEq<Float> ltEq_11 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1200.0f, true);
        And and1 = (And) FilterFactory.and(gtEq_11, ltEq_11);
        
        GtEq<Float> gtEq_12 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1200.0f, true);
        LtEq<Float> ltEq_12 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1500.0f, true);
        And and2 = (And) FilterFactory.and(gtEq_12, ltEq_12);
        
        GtEq<Float> gtEq_13 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000.0f, false);
        LtEq<Float> ltEq_l3 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2000.0f, false);
        And and3 = (And) FilterFactory.and(gtEq_13, ltEq_l3);
        
        GtEq<Float> gtEq_14 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800.0f, true);
        LtEq<Float> ltEq_14 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000.0f, true);
        And and4 = (And) FilterFactory.and(gtEq_14, ltEq_14);

        Or or1 = (Or) FilterFactory.or(and1, and2);
        SingleValueVisitor<Float> vistor = new SingleValueVisitor<>(or1);
        assertTrue(vistor.verify(1200.0f));
        
        Or or2 = (Or) FilterFactory.or(and3, and4);
        SingleValueVisitor<Float> vistor2 = new SingleValueVisitor<>(or2);
        // FloatInterval ans = (FloatInterval) new FloatFilterVerifier().getInterval(or2);
        assertTrue(vistor2.verify(1000.0f)); 
        
        GtEq<Float> gtEq_16 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000.0f, false);
        LtEq<Float> ltEq_l6 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2000.0f, false);
        And and6 = (And) FilterFactory.and(gtEq_16, ltEq_l6);
        GtEq<Float> gtEq_17 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800.0f, true);
        LtEq<Float> ltEq_17 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000.0f, true);
        And and7 = (And) FilterFactory.and(gtEq_17, ltEq_17);
        Or or7 = (Or) FilterFactory.or(and6, and7);
        FloatInterval ans7 = (FloatInterval) new FloatFilterVerifier().getInterval(or7);
        assertEquals(ans7.v[0], 800.0f, float_min_delta);
        
        GtEq<Float> gtEq_18 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1800.0f, false);
        LtEq<Float> ltEq_l8 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2000.0f, false);
        And and8 = (And) FilterFactory.and(gtEq_18, ltEq_l8);
        GtEq<Float> gtEq_19 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800.0f, true);
        LtEq<Float> ltEq_19 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 3000.0f, true);
        And and9 = (And) FilterFactory.and(gtEq_19, ltEq_19);
        Or or9 = (Or) FilterFactory.or(and8, and9);
        FloatInterval ans9 = (FloatInterval) new FloatFilterVerifier().getInterval(or9);  
        assertEquals(ans9.v[0], 800.0f, float_min_delta);
        assertEquals(ans9.v[1], 3000.0f, float_min_delta);
    }

    @Test
    public void andOrBorderTest() {
        double theta = 0.0001;

        // And Operator
        GtEq<Float> gtEq1 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0f, false);
        LtEq<Float> ltEq1 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0f, false);
        And and1 = (And) FilterFactory.and(gtEq1, ltEq1);
        FloatInterval ans = (FloatInterval) new FloatFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);
        and1 = (And) FilterFactory.and(ltEq1, gtEq1);
        ans = (FloatInterval) new FloatFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);

        gtEq1 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0f, true);
        ltEq1 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0f, false);
        and1 = (And) FilterFactory.and(gtEq1, ltEq1);
        ans = (FloatInterval) new FloatFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);
        and1 = (And) FilterFactory.and(ltEq1, gtEq1);
        ans = (FloatInterval) new FloatFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);

        gtEq1 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0f, false);
        ltEq1 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0f, true);
        and1 = (And) FilterFactory.and(gtEq1, ltEq1);
        ans = (FloatInterval) new FloatFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);
        and1 = (And) FilterFactory.and(ltEq1, gtEq1);
        ans = (FloatInterval) new FloatFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);

        // Or Operator
        gtEq1 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0f, false);
        ltEq1 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0f, false);
        Or or1 = (Or) FilterFactory.or(gtEq1, ltEq1);
        ans = (FloatInterval) new FloatFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 4);
        assertEquals(ans.v[0], -Float.MAX_VALUE, theta); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], 2L, theta); assertEquals(ans.flag[1], false);
        assertEquals(ans.v[2], 2L, theta); assertEquals(ans.flag[2], false);
        assertEquals(ans.v[3], Float.MAX_VALUE, theta); assertEquals(ans.flag[3], true);
        or1 = (Or) FilterFactory.or(ltEq1, gtEq1);
        ans = (FloatInterval) new FloatFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 4);
        assertEquals(ans.v[0], -Float.MAX_VALUE, theta); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], 2L, theta); assertEquals(ans.flag[1], false);
        assertEquals(ans.v[2], 2L, theta); assertEquals(ans.flag[2], false);
        assertEquals(ans.v[3], Float.MAX_VALUE, theta); assertEquals(ans.flag[3], true);

        gtEq1 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0f, true);
        ltEq1 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0f, false);
        or1 = (Or) FilterFactory.or(gtEq1, ltEq1);
        ans = (FloatInterval) new FloatFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 2);
        assertEquals(ans.v[0], -Float.MAX_VALUE, theta); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], Float.MAX_VALUE, theta); assertEquals(ans.flag[1], true);
        or1 = (Or) FilterFactory.or(ltEq1, gtEq1);
        ans = (FloatInterval) new FloatFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 2);
        assertEquals(ans.v[0], -Float.MAX_VALUE, theta); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], Float.MAX_VALUE, theta); assertEquals(ans.flag[1], true);

        gtEq1 = FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0f, false);
        ltEq1 = FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0f, true);
        or1 = (Or) FilterFactory.or(gtEq1, ltEq1);
        ans = (FloatInterval) new FloatFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 2);
        assertEquals(ans.v[0], -Float.MAX_VALUE, theta); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], Float.MAX_VALUE, theta); assertEquals(ans.flag[1], true);
        or1 = (Or) FilterFactory.or(ltEq1, gtEq1);
        ans = (FloatInterval) new FloatFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 2);
        assertEquals(ans.v[0], -Float.MAX_VALUE, theta); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], Float.MAX_VALUE, theta); assertEquals(ans.flag[1], true);
    }
    
}
