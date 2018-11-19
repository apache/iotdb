package cn.edu.tsinghua.tsfile.timeseries.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.NotEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.GtEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Or;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DoubleInterval;
import cn.edu.tsinghua.tsfile.timeseries.filter.verifier.DoubleFilterVerifier;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.And;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Eq;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.LtEq;

/**
 * 
 * @author CGF
 *
 */
public class FilterVerifierDoubleTest {

    private static final Logger LOG = LoggerFactory.getLogger(FilterVerifierDoubleTest.class);

    private static final double double_min_delta = 0.00001f;
	private static String deltaObjectUID = FilterTestConstant.deltaObjectUID;
	private static String measurementUID = FilterTestConstant.measurementUID;
	
    @Test
    public void eqTest() {
        Eq<Double> eq = FilterFactory
                .eq(FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45.0);
        DoubleInterval x = (DoubleInterval) new DoubleFilterVerifier().getInterval(eq);
        assertEquals(x.count, 2);
        assertEquals(x.v[0], 45, double_min_delta);
        assertEquals(x.v[1], 45, double_min_delta);
    }

    @Test
    public void ltEqTest() {
        LtEq<Double> ltEq = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45.0, true);
        DoubleInterval x = (DoubleInterval) new DoubleFilterVerifier().getInterval(ltEq);
        assertEquals(x.count, 2);
        assertEquals(x.v[0], -Double.MAX_VALUE, double_min_delta);
        assertEquals(x.v[1], 45.0f, double_min_delta);

        ltEq = FilterFactory.ltEq(FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), -45.0, true);
        SingleValueVisitor<Float> visitor = new SingleValueVisitor<>(ltEq);
        Assert.assertTrue(visitor.verify(-46.0));
        Assert.assertFalse(visitor.verify(-40.0));
        Assert.assertFalse(visitor.verify(70.0));
    }

    @Test
    public void andOrTest() {
        // [470,1200) & (500,800]|[1000,2000)

        GtEq<Double> gtEq1 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 470.0, true);
        LtEq<Double> ltEq1 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1200.0, false);
        And and1 = (And) FilterFactory.and(gtEq1, ltEq1);

        GtEq<Double> gtEq2 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500.0, false);
        LtEq<Double> ltEq2 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800.0, true);
        And and2 = (And) FilterFactory.and(gtEq2, ltEq2);

        GtEq<Double> gtEq3 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000.0, true);
        LtEq<Double> ltEq3 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2000.0, false);
        And and3 = (And) FilterFactory.and(gtEq3, ltEq3);
        Or or1 = (Or) FilterFactory.or(and2, and3);

        And andCombine1 = (And) FilterFactory.and(and1, or1);
        DoubleInterval ans = (DoubleInterval) new DoubleFilterVerifier().getInterval(andCombine1);

        LOG.info("and+Or Test");
        assertEquals(ans.count, 4);
        assertEquals(ans.v[0], 500.0f, double_min_delta);
        assertEquals(ans.flag[0], false);
        assertEquals(ans.v[1], 800.0f, double_min_delta);
        assertEquals(ans.flag[1], true);
        assertEquals(ans.v[2], 1000.0f, double_min_delta);
        assertEquals(ans.flag[2], true);
        assertEquals(ans.v[3], 1200.0f, double_min_delta);
        assertEquals(ans.flag[3], false);


        // for filter test coverage
        // [400, 500) (600, 800]
        GtEq<Double> gtEq4 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 400.0, true);
        LtEq<Double> ltEq4 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500.0, false);
        And and4 = (And) FilterFactory.and(gtEq4, ltEq4);

        GtEq<Double> gtEq5 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 600.0, false);
        LtEq<Double> ltEq5 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800.0, true);
        And and5 = (And) FilterFactory.and(gtEq5, ltEq5);

        And andNew = (And) FilterFactory.and(and4, and5);
        DoubleInterval ansNew = (DoubleInterval) new DoubleFilterVerifier().getInterval(andNew);
        assertEquals(ansNew.count, 0);

        // for filter test coverage2
        // [600, 800] [400, 500]
        GtEq<Double> gtEq6 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 600.0, true);
        LtEq<Double> ltEq6 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800.0, false);
        And and6 = (And) FilterFactory.and(gtEq6, ltEq6);

        GtEq<Double> gtEq7 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 400.0, false);
        LtEq<Double> ltEq8 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500.0, true);
        And and7 = (And) FilterFactory.and(gtEq7, ltEq8);

        And andCombine3 = (And) FilterFactory.and(and6, and7);
        DoubleInterval intervalAns =
                (DoubleInterval) new DoubleFilterVerifier().getInterval(andCombine3);
        assertEquals(intervalAns.count, 0);
    }

    @Test
    public void notEqTest() {
        NotEq<Double> notEq = FilterFactory
                .noteq(FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000.0);
        DoubleInterval ans = (DoubleInterval) new DoubleFilterVerifier().getInterval(notEq);

        assertEquals(ans.count, 4);
        assertEquals(ans.v[0], -Double.MAX_VALUE, double_min_delta);
        assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], 1000.0f, double_min_delta);
        assertEquals(ans.flag[1], false);
        assertEquals(ans.v[2], 1000.0f, double_min_delta);
        assertEquals(ans.flag[2], false);
        assertEquals(ans.v[3], Double.MAX_VALUE, double_min_delta);
        assertEquals(ans.flag[3], true);
    }

    @Test
    public void orTest() {
        // [470,1200) | (500,800] | [1000,2000) | [100,200]

        GtEq<Double> gtEq_11 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 470.0, true);
        LtEq<Double> ltEq_11 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1200.0, false);
        And and1 = (And) FilterFactory.and(gtEq_11, ltEq_11);

        GtEq<Double> gtEq_12 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500.0, false);
        LtEq<Double> ltEq_12 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800.0, true);
        And and2 = (And) FilterFactory.and(gtEq_12, ltEq_12);

        GtEq<Double> gtEq_13 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000.0, true);
        LtEq<Double> ltEq_l3 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2000.0, false);
        And and3 = (And) FilterFactory.and(gtEq_13, ltEq_l3);

        GtEq<Double> gtEq_14 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 100.0, true);
        LtEq<Double> ltEq_14 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 200.0, true);
        And and4 = (And) FilterFactory.and(gtEq_14, ltEq_14);

        Or or1 = (Or) FilterFactory.or(and2, and3);
        Or or2 = (Or) FilterFactory.or(or1, and4);

        Or orCombine = (Or) FilterFactory.or(and1, or2);
        //DoubleInterval ans = (DoubleInterval) new DoubleFilterVerifier().getInterval(orCombine);
        LOG.info("all : or Test output");

        // answer may have overlap, but is right
        SingleValueVisitor<Double> vistor = new SingleValueVisitor<>(orCombine);
        assertTrue(vistor.verify(500.0));
        assertTrue(vistor.verify(600.0));
        assertTrue(vistor.verify(1199.0));
        assertTrue(vistor.verify(1999.0));
        assertFalse(vistor.verify(5.0));
        assertFalse(vistor.verify(2000.0));
        assertFalse(vistor.verify(469.0));
        assertFalse(vistor.verify(99.0));
        assertTrue(vistor.verify(100.0));
        assertTrue(vistor.verify(200.0));
        assertFalse(vistor.verify(201.0));

    }

    @Test
    public void orborderTest() {
        // [470,1200] | [1200, 1500]

        GtEq<Double> gtEq_11 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 470.0, true);
        LtEq<Double> ltEq_11 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1200.0, true);
        And and1 = (And) FilterFactory.and(gtEq_11, ltEq_11);

        GtEq<Double> gtEq_12 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1200.0, true);
        LtEq<Double> ltEq_12 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1500.0, true);
        And and2 = (And) FilterFactory.and(gtEq_12, ltEq_12);

        GtEq<Double> gtEq_13 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000.0, false);
        LtEq<Double> ltEq_l3 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2000.0, false);
        And and3 = (And) FilterFactory.and(gtEq_13, ltEq_l3);

        GtEq<Double> gtEq_14 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800.0, true);
        LtEq<Double> ltEq_14 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000.0, true);
        And and4 = (And) FilterFactory.and(gtEq_14, ltEq_14);

        Or or1 = (Or) FilterFactory.or(and1, and2);
        SingleValueVisitor<Double> vistor = new SingleValueVisitor<>(or1);
        assertTrue(vistor.verify(1200.0));

        Or or2 = (Or) FilterFactory.or(and3, and4);
        SingleValueVisitor<Double> vistor2 = new SingleValueVisitor<>(or2);
        // DoubleInterval ans = (DoubleInterval) new DoubleFilterVerifier().getInterval(or2);
        assertTrue(vistor2.verify(1000.0));

        GtEq<Double> gtEq_16 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000.0, false);
        LtEq<Double> ltEq_l6 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2000.0, false);
        And and6 = (And) FilterFactory.and(gtEq_16, ltEq_l6);
        GtEq<Double> gtEq_17 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800.0, true);
        LtEq<Double> ltEq_17 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000.0, true);
        And and7 = (And) FilterFactory.and(gtEq_17, ltEq_17);
        Or or7 = (Or) FilterFactory.or(and6, and7);
        DoubleInterval ans7 = (DoubleInterval) new DoubleFilterVerifier().getInterval(or7);
        assertEquals(ans7.v[0], 800.0f, double_min_delta);

        GtEq<Double> gtEq_18 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1800.0, false);
        LtEq<Double> ltEq_l8 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2000.0, false);
        And and8 = (And) FilterFactory.and(gtEq_18, ltEq_l8);
        GtEq<Double> gtEq_19 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800.0, true);
        LtEq<Double> ltEq_19 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 3000.0, true);
        And and9 = (And) FilterFactory.and(gtEq_19, ltEq_19);
        Or or9 = (Or) FilterFactory.or(and8, and9);
        DoubleInterval ans9 = (DoubleInterval) new DoubleFilterVerifier().getInterval(or9);
        assertEquals(ans9.v[0], 800.0, double_min_delta);
        assertEquals(ans9.v[1], 3000.0, double_min_delta);

    }
    
    @Test
    public void bugTest1() {
        // [470,1200] | [1200, 1500]

        GtEq<Double> gtEq_11 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 470.0, true);
        LtEq<Double> ltEq_11 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1200.0, true);
        And and1 = (And) FilterFactory.and(gtEq_11, ltEq_11);

        GtEq<Double> gtEq_12 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1200.0, true);
        LtEq<Double> ltEq_12 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1500.0, true);
        And and2 = (And) FilterFactory.and(gtEq_12, ltEq_12);

        And and = (And) FilterFactory.and(and1, and2);
        DoubleInterval interval = (DoubleInterval) new DoubleFilterVerifier().getInterval(and);
        assertEquals(interval.v[0], 1200.0, double_min_delta);
        assertEquals(interval.v[1], 1200.0, double_min_delta);

    }
    
    @Test
    public void bugTest2() {
        // [470,1200] | [1200, 1500]

        GtEq<Double> gtEq_11 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 100.0, true);
        LtEq<Double> ltEq_11 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 200.0, true);
        And and1 = (And) FilterFactory.and(gtEq_11, ltEq_11);

        GtEq<Double> gtEq_12 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 20.0, true);
        LtEq<Double> ltEq_12 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 100.0, true);
        And and2 = (And) FilterFactory.and(gtEq_12, ltEq_12);

        And and = (And) FilterFactory.and(and1, and2);
        DoubleInterval interval = (DoubleInterval) new DoubleFilterVerifier().getInterval(and);
        assertEquals(interval.v[0], 100.0, double_min_delta);

    }
    
    @Test
    public void bugTest3() {
        // [470,1200] | [1200, 1500]

        GtEq<Double> gtEq_11 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 100.0, true);
        LtEq<Double> ltEq_11 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 200.0, true);
        And and1 = (And) FilterFactory.and(gtEq_11, ltEq_11);

        GtEq<Double> gtEq_12 = FilterFactory.gtEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 100.0, true);
        LtEq<Double> ltEq_12 = FilterFactory.ltEq(
                FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 200.0, true);
        And and2 = (And) FilterFactory.and(gtEq_12, ltEq_12);

        Or or = (Or) FilterFactory.or(and1, and2);
        DoubleInterval interval = (DoubleInterval) new DoubleFilterVerifier().getInterval(or);
        assertEquals(interval.v[0], 100.0, double_min_delta);
        assertEquals(interval.v[1], 200.0, double_min_delta);

    }

    @Test
    public void andOrBorderTest() {
        double theta = 0.0001;

        // And Operator
        GtEq<Double> gtEq1 = FilterFactory.gtEq(FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0, false);
        LtEq<Double> ltEq1 = FilterFactory.ltEq(FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0, false);
        And and1 = (And) FilterFactory.and(gtEq1, ltEq1);
        DoubleInterval ans = (DoubleInterval) new DoubleFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);
        and1 = (And) FilterFactory.and(ltEq1, gtEq1);
        ans = (DoubleInterval) new DoubleFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);

        gtEq1 = FilterFactory.gtEq(FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0, true);
        ltEq1 = FilterFactory.ltEq(FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0, false);
        and1 = (And) FilterFactory.and(gtEq1, ltEq1);
        ans = (DoubleInterval) new DoubleFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);
        and1 = (And) FilterFactory.and(ltEq1, gtEq1);
        ans = (DoubleInterval) new DoubleFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);

        gtEq1 = FilterFactory.gtEq(FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0, false);
        ltEq1 = FilterFactory.ltEq(FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0, true);
        and1 = (And) FilterFactory.and(gtEq1, ltEq1);
        ans = (DoubleInterval) new DoubleFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);
        and1 = (And) FilterFactory.and(ltEq1, gtEq1);
        ans = (DoubleInterval) new DoubleFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);

        // Or Operator
        gtEq1 = FilterFactory.gtEq(FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0, false);
        ltEq1 = FilterFactory.ltEq(FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0, false);
        Or or1 = (Or) FilterFactory.or(gtEq1, ltEq1);
        ans = (DoubleInterval) new DoubleFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 4);
        assertEquals(ans.v[0], -Double.MAX_VALUE, theta); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], 2L, theta); assertEquals(ans.flag[1], false);
        assertEquals(ans.v[2], 2L, theta); assertEquals(ans.flag[2], false);
        assertEquals(ans.v[3], Double.MAX_VALUE, theta); assertEquals(ans.flag[3], true);
        or1 = (Or) FilterFactory.or(ltEq1, gtEq1);
        ans = (DoubleInterval) new DoubleFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 4);
        assertEquals(ans.v[0], -Double.MAX_VALUE, theta); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], 2L, theta); assertEquals(ans.flag[1], false);
        assertEquals(ans.v[2], 2L, theta); assertEquals(ans.flag[2], false);
        assertEquals(ans.v[3], Double.MAX_VALUE, theta); assertEquals(ans.flag[3], true);

        gtEq1 = FilterFactory.gtEq(FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0, true);
        ltEq1 = FilterFactory.ltEq(FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0, false);
        or1 = (Or) FilterFactory.or(gtEq1, ltEq1);
        ans = (DoubleInterval) new DoubleFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 2);
        assertEquals(ans.v[0], -Double.MAX_VALUE, theta); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], Double.MAX_VALUE, theta); assertEquals(ans.flag[1], true);
        or1 = (Or) FilterFactory.or(ltEq1, gtEq1);
        ans = (DoubleInterval) new DoubleFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 2);
        assertEquals(ans.v[0], -Double.MAX_VALUE, theta); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], Double.MAX_VALUE, theta); assertEquals(ans.flag[1], true);

        gtEq1 = FilterFactory.gtEq(FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0, false);
        ltEq1 = FilterFactory.ltEq(FilterFactory.doubleFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2.0, true);
        or1 = (Or) FilterFactory.or(gtEq1, ltEq1);
        ans = (DoubleInterval) new DoubleFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 2);
        assertEquals(ans.v[0], -Double.MAX_VALUE, theta); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], Double.MAX_VALUE, theta); assertEquals(ans.flag[1], true);
        or1 = (Or) FilterFactory.or(ltEq1, gtEq1);
        ans = (DoubleInterval) new DoubleFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 2);
        assertEquals(ans.v[0], -Double.MAX_VALUE, theta); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], Double.MAX_VALUE, theta); assertEquals(ans.flag[1], true);
    }
    
}
