package cn.edu.tsinghua.tsfile.timeseries.filter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.NotEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitorFactory;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.And;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Eq;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.GtEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.LtEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Not;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Or;

/**
 * 
 * @author CGF
 *
 */
public class SingleValueVisitorTest {
    
    private static final SingleValueVisitor<?> int32Vistor = SingleValueVisitorFactory.getSingleValueVisitor(TSDataType.INT32);
	private static String deltaObjectUID = FilterTestConstant.deltaObjectUID;
	private static String measurementUID = FilterTestConstant.measurementUID;
    
    @Test
    public void genericErrorTest() {
        Eq<Integer> eq = FilterFactory.eq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45);
        SingleValueVisitor<?> vistor = SingleValueVisitorFactory.getSingleValueVisitor(TSDataType.INT32);
        //System.out.println(vistor.satisfyObject(10.0, eq));
        assertFalse(vistor.satisfyObject(45L, eq));
    }
    
    @Test 
    public void egTest() {
        Eq<Integer> eq = FilterFactory.eq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45);
        assertFalse(int32Vistor.satisfyObject(10, eq));
        assertTrue(int32Vistor.satisfyObject(45, eq));  
    }
    
    @Test 
    public void noteqTest() {
        NotEq<Integer> noteq = FilterFactory.noteq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45);
        assertTrue(int32Vistor.satisfyObject(10, noteq));
        assertFalse(int32Vistor.satisfyObject(45, noteq));  
    }
    
    @Test 
    public void ltTest() {
        LtEq<Integer> lteq1 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45, true);
        assertTrue(int32Vistor.satisfyObject(45, lteq1));
        assertTrue(int32Vistor.satisfyObject(44, lteq1));
        assertTrue(int32Vistor.satisfyObject(0, lteq1));
        assertFalse(int32Vistor.satisfyObject(46, lteq1));  
        assertFalse(int32Vistor.satisfyObject(146, lteq1));
        
        LtEq<Integer> lteq2 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45, false);
        assertTrue(int32Vistor.satisfyObject(44, lteq2));
        assertTrue(int32Vistor.satisfyObject(0, lteq2));
        assertFalse(int32Vistor.satisfyObject(45, lteq2));
        assertFalse(int32Vistor.satisfyObject(46, lteq2));  
        assertFalse(int32Vistor.satisfyObject(146, lteq2));
    }
    
    @Test 
    public void gtTest() {
        GtEq<Integer> gteq1 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45, true);
        assertTrue(int32Vistor.satisfyObject(45, gteq1));
        assertTrue(int32Vistor.satisfyObject(46, gteq1));  
        assertTrue(int32Vistor.satisfyObject(146, gteq1));
        assertFalse(int32Vistor.satisfyObject(44, gteq1));
        assertFalse(int32Vistor.satisfyObject(0, gteq1));

        GtEq<Integer> gteq2 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45, false);
        assertTrue(int32Vistor.satisfyObject(46, gteq2));  
        assertTrue(int32Vistor.satisfyObject(146, gteq2));
        assertFalse(int32Vistor.satisfyObject(44, gteq2));
        assertFalse(int32Vistor.satisfyObject(0, gteq2));
        assertFalse(int32Vistor.satisfyObject(45, gteq2));
    }
    
    @Test 
    public void notTest() {
        GtEq<Integer> gteq1 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45, true);
        Not not = (Not) FilterFactory.not(gteq1);  // < 45
        assertTrue(int32Vistor.satisfyObject(44, not));
        assertTrue(int32Vistor.satisfyObject(0, not));
        assertFalse(int32Vistor.satisfyObject(45, not));
        assertFalse(int32Vistor.satisfyObject(46, not));  
        assertFalse(int32Vistor.satisfyObject(146, not));
    }
    
    @Test 
    public void andTest() {
        GtEq<Integer> gteq = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 5, true);
        LtEq<Integer> lteq = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45, false);
        And and = (And) FilterFactory.and(gteq, lteq);
        assertTrue(int32Vistor.satisfyObject(5, and));
        assertTrue(int32Vistor.satisfyObject(44, and));
        assertTrue(int32Vistor.satisfyObject(40, and));
        assertFalse(int32Vistor.satisfyObject(45, and));
        assertFalse(int32Vistor.satisfyObject(46, and));
        assertFalse(int32Vistor.satisfyObject(11115, and));
    }
    
    @Test 
    public void orTest() {
        GtEq<Integer> gteq = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 550, false);
        LtEq<Integer> lteq = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 52, true);
        Or or = (Or) FilterFactory.or(gteq, lteq);
        
        assertTrue(int32Vistor.satisfyObject(551, or));
        assertTrue(int32Vistor.satisfyObject(5500, or));
        assertTrue(int32Vistor.satisfyObject(52, or));
        assertTrue(int32Vistor.satisfyObject(51, or));
        assertTrue(int32Vistor.satisfyObject(5, or));
        assertFalse(int32Vistor.satisfyObject(550, or));
        assertFalse(int32Vistor.satisfyObject(53, or));
    }
}
