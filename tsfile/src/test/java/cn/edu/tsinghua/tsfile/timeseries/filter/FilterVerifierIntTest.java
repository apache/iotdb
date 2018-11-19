package cn.edu.tsinghua.tsfile.timeseries.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.LtEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.NotEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.GtEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Not;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Or;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.IntInterval;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.And;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Eq;
import cn.edu.tsinghua.tsfile.timeseries.filter.verifier.IntFilterVerifier;

/**
 * 
 * @author CGF
 *
 */
public class FilterVerifierIntTest { 
	
	private static String deltaObjectUID = "d";
	private static String measurementUID = "s";
	
    @Test
    public void eqTest() {
        Eq<Integer> eq = FilterFactory.eq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45);
        IntInterval x = (IntInterval) new IntFilterVerifier().getInterval(eq);
        assertEquals(x.count, 2);
        assertEquals(x.v[0], 45);
        assertEquals(x.v[1], 45);
    }
    
    @Test
    public void ltEqTest() {
        LtEq<Integer> ltEq = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45, true);
        IntInterval x= (IntInterval) new IntFilterVerifier().getInterval(ltEq);
        assertEquals(x.count, 2);
        assertEquals(x.v[0], Integer.MIN_VALUE);
        assertEquals(x.v[1], 45);
    }
    
    @Test
    public void andOrTest() {
        // [470,1200) & (500,800]|[1000,2000)  ans:(500,800], [1000,1200)
        
        GtEq<Integer> gtEq1 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 470, true);
        LtEq<Integer> ltEq1 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1200, false);
        And and1 = (And) FilterFactory.and(gtEq1, ltEq1);
        
        GtEq<Integer> gtEq2 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500, false);
        LtEq<Integer> ltEq2 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800, true);
        And and2 = (And) FilterFactory.and(gtEq2, ltEq2);
        
        GtEq<Integer> gtEq3 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000, true);
        LtEq<Integer> ltEq3 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2000, false);
        And and3 = (And) FilterFactory.and(gtEq3, ltEq3);
        Or or1 = (Or) FilterFactory.or(and2, and3);
        
        And andCombine1 = (And) FilterFactory.and(and1, or1);
        IntInterval ans = (IntInterval) new IntFilterVerifier().getInterval(andCombine1);
        System.out.println(ans);
        // ans.output();
        assertEquals(ans.count, 4);
        assertEquals(ans.v[0], 500);
        assertEquals(ans.flag[0], false);
        assertEquals(ans.v[1], 800);
        assertEquals(ans.flag[1], true);
        assertEquals(ans.v[2], 1000); 
        assertEquals(ans.flag[2], true);
        assertEquals(ans.v[3], 1200);
        assertEquals(ans.flag[3], false);
        
        // for filter test coverage
        // [400, 500) (600, 800]
        GtEq<Integer> gtEq4 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 400, true);
        LtEq<Integer> ltEq4 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500, false);
        And and4 = (And) FilterFactory.and(gtEq4, ltEq4);
        
        GtEq<Integer> gtEq5 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 600, false);
        LtEq<Integer> ltEq5 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800, true);
        And and5 = (And) FilterFactory.and(gtEq5, ltEq5);
        
        And andNew = (And) FilterFactory.and(and4, and5);
        IntInterval ansNew = (IntInterval) new IntFilterVerifier().getInterval(andNew);
        
        assertEquals(ansNew.count, 0);
        
        // for filter test coverage2
        // [600, 800] [400, 500] 
        GtEq<Integer> gtEq6 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 600, true);
        LtEq<Integer> ltEq6 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800, false);
        And and6 = (And) FilterFactory.and(gtEq6, ltEq6);
        
        GtEq<Integer> gtEq7 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 400, false);
        LtEq<Integer> ltEq8 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500, true);
        And and7 = (And) FilterFactory.and(gtEq7, ltEq8);
        
        And andCombine3 = (And) FilterFactory.and(and6, and7);
        
        IntInterval intervalAns = (IntInterval) new IntFilterVerifier().getInterval(andCombine3);
        
        assertEquals(intervalAns.count, 0);
    }
    
    @Test
    public void notEqTest() {
        NotEq<Integer> notEq = FilterFactory.noteq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000);
        IntInterval ans = (IntInterval) new IntFilterVerifier().getInterval(notEq);
        
        assertEquals(ans.count, 4);
        assertEquals(ans.v[0], Integer.MIN_VALUE);
        assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], 1000);
        assertEquals(ans.flag[1], false);
        assertEquals(ans.v[2], 1000);
        assertEquals(ans.flag[2], false);
        assertEquals(ans.v[3], Integer.MAX_VALUE);
        assertEquals(ans.flag[3], true);
    }
    
    @Test
    public void orTest() {
        // [470,1200) | (500,800] | [1000,2000) | [100,200]  ===>  ans:[100,200], [470,2000)
        
        GtEq<Integer> gtEq_11 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 470, true);
        LtEq<Integer> ltEq_11 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1200, false);
        And and1 = (And) FilterFactory.and(gtEq_11, ltEq_11);
        
        GtEq<Integer> gtEq_12 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500, false);
        LtEq<Integer> ltEq_12 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800, true);
        And and2 = (And) FilterFactory.and(gtEq_12, ltEq_12);
        
        GtEq<Integer> gtEq_13 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000, true);
        LtEq<Integer> ltEq_l3 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2000, false);
        And and3 = (And) FilterFactory.and(gtEq_13, ltEq_l3);
        
        GtEq<Integer> gtEq_14 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 100, true);
        LtEq<Integer> ltEq_14 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 200, true);
        And and4 = (And) FilterFactory.and(gtEq_14, ltEq_14);
        
        Or o1 = (Or) FilterFactory.or(and2, and3);
        Or o2 = (Or) FilterFactory.or(o1, and4);
        
        Or or = (Or) FilterFactory.or(and1, o2);
        IntInterval ans = (IntInterval) new IntFilterVerifier().getInterval(o2);
        System.out.println(ans);
        
        SingleValueVisitor<Integer> vistor = new SingleValueVisitor<>(or);
        assertTrue(vistor.verify(500));
        assertTrue(vistor.verify(600));
        assertTrue(vistor.verify(1199));
        assertTrue(vistor.verify(1999));
        assertFalse(vistor.verify(5));
        assertFalse(vistor.verify(2000));
        assertFalse(vistor.verify(469));
        assertFalse(vistor.verify(99));
        assertTrue(vistor.verify(100));
        assertTrue(vistor.verify(200));
        assertFalse(vistor.verify(201));
        
    }
    
    @Test
    public void additionTest() {
        
        GtEq<Integer> gtEq_11 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 4000, true);
        LtEq<Integer> ltEq_11 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 4500, false);
        And and1 = (And) FilterFactory.and(gtEq_11, ltEq_11);
        
        GtEq<Integer> gtEq_12 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500, false);
        LtEq<Integer> ltEq_12 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800, true);
        And and2 = (And) FilterFactory.and(gtEq_12, ltEq_12);
        
        And and = (And) FilterFactory.and(and1, and2);
        IntInterval ans = (IntInterval) new IntFilterVerifier().getInterval(and);
        assertEquals(ans.count, 0);
    }
    
    @Test
    public void notOperatorTest() {
    	
    	// [30,600) | (500,800] | [1000,2000) | [100,200]
    	
    	GtEq<Integer> gtEq_11 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 30, true);
        LtEq<Integer> ltEq_11 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 600, false);
        //GtEq<Integer> gtEq_112 = FilterApi.gtEq(FilterApi.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 600, true);
        //LtEq<Integer> ltEq_112 = FilterApi.ltEq(FilterApi.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 30, false);
        And and1 = (And) FilterFactory.and(gtEq_11, ltEq_11);
        //Or or1 = (Or) FilterApi.or(gtEq_112, ltEq_112);
        
        GtEq<Integer> gtEq_12 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500, false);
        LtEq<Integer> ltEq_12 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800, true);
        //GtEq<Integer> gtEq_122 = FilterApi.gtEq(FilterApi.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800, false);
        //LtEq<Integer> ltEq_122 = FilterApi.ltEq(FilterApi.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500, true);
        And and2 = (And) FilterFactory.and(gtEq_12, ltEq_12);
        //Or or2 = (Or) FilterApi.or(gtEq_122, ltEq_122);
        
        GtEq<Integer> gtEq_13 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1000, true);
        LtEq<Integer> ltEq_l3 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2000, false);
        And and3 = (And) FilterFactory.and(gtEq_13, ltEq_l3);
        
        GtEq<Integer> gtEq_14 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 100, true);
        LtEq<Integer> ltEq_14 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 200, true);
        And and4 = (And) FilterFactory.and(gtEq_14, ltEq_14);
        
        
        Or or_12 = (Or) FilterFactory.or(and1, and2);
        Or or_34 = (Or) FilterFactory.or(and3, and4);
        Or or = (Or) FilterFactory.or(or_12, or_34);
        Not notAll = (Not) FilterFactory.not(or);
        //IntInterval ans = (IntInterval) new IntFilterVerifier().getInterval(notAll);
        
        SingleValueVisitor<Integer> vistor = new SingleValueVisitor<>(notAll);
        assertFalse(vistor.verify(500));
        assertFalse(vistor.verify(600));
        assertFalse(vistor.verify(1199));
        assertFalse(vistor.verify(1999));
        assertTrue(vistor.verify(5));
        assertTrue(vistor.verify(2000));
        assertFalse(vistor.verify(469));
        assertFalse(vistor.verify(99));
        assertFalse(vistor.verify(100));
        assertFalse(vistor.verify(200));
        assertFalse(vistor.verify(201));
    }
    
    @Test
    public void minMaxValueTest() {
        
        GtEq<Integer> gtEq1 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 
        		Integer.MAX_VALUE, true);
        IntInterval i = (IntInterval) new IntFilterVerifier().getInterval(gtEq1);
        assertEquals(i.v[0], Integer.MAX_VALUE);
        assertEquals(i.v[1], Integer.MAX_VALUE);
        assertEquals(i.flag[0], true);
        assertEquals(i.flag[1], true);
        
        GtEq<Integer> gtEq2 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 
        		Integer.MAX_VALUE, false);
        IntInterval i2 = (IntInterval) new IntFilterVerifier().getInterval(gtEq2);
        assertEquals(i2.v[0], Integer.MAX_VALUE);
        assertEquals(i2.v[1], Integer.MAX_VALUE);
        assertEquals(i2.flag[0], false);
        assertEquals(i2.flag[1], false);
        
        LtEq<Integer> ltEq1 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 
        		Integer.MIN_VALUE, true);
        IntInterval i3 = (IntInterval) new IntFilterVerifier().getInterval(ltEq1);
        assertEquals(i3.v[0], Integer.MIN_VALUE);
        assertEquals(i3.v[1], Integer.MIN_VALUE);
        assertEquals(i3.flag[0], true);
        assertEquals(i3.flag[1], true);
        
        LtEq<Integer> ltEq2 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 
        		Integer.MIN_VALUE, false);
        IntInterval i4 = (IntInterval) new IntFilterVerifier().getInterval(ltEq2);
        assertEquals(i4.v[0], Integer.MIN_VALUE);
        assertEquals(i4.v[1], Integer.MIN_VALUE);
        assertEquals(i4.flag[0], false);
        assertEquals(i4.flag[1], false);
        
        NotEq<Integer> noteq = FilterFactory.noteq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 
        		Integer.MIN_VALUE);
        IntInterval i5 = (IntInterval) new IntFilterVerifier().getInterval(noteq);
        assertEquals(i5.v[0], Integer.MIN_VALUE);
        assertEquals(i5.v[1], Integer.MIN_VALUE);
        assertEquals(i5.v[2], Integer.MIN_VALUE);
        assertEquals(i5.v[3], Integer.MAX_VALUE);
        assertEquals(i5.flag[0], false);
        assertEquals(i5.flag[1], false);
        assertEquals(i5.flag[2], false);
        assertEquals(i5.flag[3], true);
        
        NotEq<Integer> noteq2 = FilterFactory.noteq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 
        		Integer.MAX_VALUE);
        IntInterval i6 = (IntInterval) new IntFilterVerifier().getInterval(noteq2);
        assertEquals(i6.v[0], Integer.MIN_VALUE);
        assertEquals(i6.v[1], Integer.MAX_VALUE);
        assertEquals(i6.v[2], Integer.MAX_VALUE);
        assertEquals(i6.v[3], Integer.MAX_VALUE);
        assertEquals(i6.flag[0], true);
        assertEquals(i6.flag[1], false);
        assertEquals(i6.flag[2], false);
        assertEquals(i6.flag[3], false);
    }
    
    @Test
    public void unionBugTest() {
        
        GtEq<Integer> gtEq1 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500, true);
        LtEq<Integer> ltEq1 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800, false);
        And and1 = (And) FilterFactory.and(gtEq1, ltEq1);
        
        GtEq<Integer> gtEq2 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 200, false);
        LtEq<Integer> ltEq2 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 300, true);
        And and2 = (And) FilterFactory.and(gtEq2, ltEq2);
        
        GtEq<Integer> gtEq3 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 350, false);
        LtEq<Integer> ltEq3 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 400, true);
        And and3 = (And) FilterFactory.and(gtEq3, ltEq3);
        Or o1 = (Or) FilterFactory.or(and2, and3);
        
        Or or = (Or) FilterFactory.or(and1, o1);
        IntInterval ans = (IntInterval) new IntFilterVerifier().getInterval(or);
        System.out.println(ans);
    }
    
    @Test
    public void unionCoverageTest() {
        
    	// right first cross
        GtEq<Integer> gtEq1 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500, true);
        LtEq<Integer> ltEq1 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800, false);
        And and1 = (And) FilterFactory.and(gtEq1, ltEq1);
        
        GtEq<Integer> gtEq2 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 200, false);
        LtEq<Integer> ltEq2 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 600, true);
        And and2 = (And) FilterFactory.and(gtEq2, ltEq2);
   
        Or o1 = (Or) FilterFactory.or(and1, and2);
        IntInterval ans = (IntInterval) new IntFilterVerifier().getInterval(o1);
        assertEquals(ans.v[0], 200); assertEquals(ans.flag[0], false);
        assertEquals(ans.v[1], 800); assertEquals(ans.flag[1], false);
        
    	// right covers left
        GtEq<Integer> gtEq3 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 500, true);
        LtEq<Integer> ltEq3 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800, false);
        And and3 = (And) FilterFactory.and(gtEq3, ltEq3);
        
        GtEq<Integer> gtEq4 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 200, false);
        LtEq<Integer> ltEq4 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 1600, true);
        And and4 = (And) FilterFactory.and(gtEq4, ltEq4);
   
        Or o2 = (Or) FilterFactory.or(and3, and4);
        IntInterval ans2 = (IntInterval) new IntFilterVerifier().getInterval(o2);
        // System.out.println(ans2);
        assertEquals(ans2.v[0], 200); assertEquals(ans2.flag[0], false);
        assertEquals(ans2.v[1], 1600); assertEquals(ans2.flag[1], true);
        
        // right first cross (2)
        GtEq<Integer> gtEq5 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 700, true);
        LtEq<Integer> ltEq5 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800, false);
        And and5 = (And) FilterFactory.and(gtEq5, ltEq5);
        
        GtEq<Integer> gtEq6 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 200, false);
        LtEq<Integer> ltEq6 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800, true);
        And and6 = (And) FilterFactory.and(gtEq6, ltEq6);
   
        Or o3 = (Or) FilterFactory.or(and5, and6);
        IntInterval ans3 = (IntInterval) new IntFilterVerifier().getInterval(o3);
        //System.out.println(ans3);
        assertEquals(ans3.v[0], 200); assertEquals(ans3.flag[0], false);
        assertEquals(ans3.v[1], 800); assertEquals(ans3.flag[1], true);
        
        // left first cross (2)
        GtEq<Integer> gtEq7 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 200, true);
        LtEq<Integer> ltEq7 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800, false);
        And and7 = (And) FilterFactory.and(gtEq7, ltEq7);
        
        GtEq<Integer> gtEq8 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 700, false);
        LtEq<Integer> ltEq8 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800, true);
        And and8 = (And) FilterFactory.and(gtEq8, ltEq8);
   
        Or o4 = (Or) FilterFactory.or(and7, and8);
        IntInterval ans4 = (IntInterval) new IntFilterVerifier().getInterval(o4);
        System.out.println(ans4);
        assertEquals(ans4.v[0], 200); assertEquals(ans4.flag[0], true);
        assertEquals(ans4.v[1], 800); assertEquals(ans4.flag[1], true);
        
        // right first
        GtEq<Integer> gtEq9 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 600, true);
        LtEq<Integer> ltEq9 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800, true);
        And and9 = (And) FilterFactory.and(gtEq9, ltEq9);
        
        GtEq<Integer> gtEq10 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 600, false);
        LtEq<Integer> ltEq10 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 700, true);
        And and10 = (And) FilterFactory.and(gtEq10, ltEq10);
   
        Or o5 = (Or) FilterFactory.or(and9, and10);
        IntInterval ans5 = (IntInterval) new IntFilterVerifier().getInterval(o5);
        System.out.println(ans4);
        assertEquals(ans5.v[0], 600); assertEquals(ans5.flag[0], true);
        assertEquals(ans5.v[1], 800); assertEquals(ans5.flag[1], true);
        
        // left first cross (1)
        GtEq<Integer> gtEq11 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 600, true);
        LtEq<Integer> ltEq11 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 700, true);
        And and11 = (And) FilterFactory.and(gtEq11, ltEq11);
        
        GtEq<Integer> gtEq12 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 600, false);
        LtEq<Integer> ltEq12 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 800, true);
        And and12 = (And) FilterFactory.and(gtEq12, ltEq12);
   
        Or o6 = (Or) FilterFactory.or(and11, and12);
        IntInterval ans6 = (IntInterval) new IntFilterVerifier().getInterval(o6);
        System.out.println(ans4);
        assertEquals(ans6.v[0], 600); assertEquals(ans6.flag[0], true);
        assertEquals(ans6.v[1], 800); assertEquals(ans6.flag[1], true);
    }

    @Test
    public void andOrBorderTest() {

        // And Operator
        GtEq<Integer> gtEq1 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2, false);
        LtEq<Integer> ltEq1 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2, false);
        And and1 = (And) FilterFactory.and(gtEq1, ltEq1);
        IntInterval ans = (IntInterval) new IntFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);
        and1 = (And) FilterFactory.and(ltEq1, gtEq1);
        ans = (IntInterval) new IntFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);

        gtEq1 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2, true);
        ltEq1 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2, false);
        and1 = (And) FilterFactory.and(gtEq1, ltEq1);
        ans = (IntInterval) new IntFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);
        and1 = (And) FilterFactory.and(ltEq1, gtEq1);
        ans = (IntInterval) new IntFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);

        gtEq1 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2, false);
        ltEq1 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2, true);
        and1 = (And) FilterFactory.and(gtEq1, ltEq1);
        ans = (IntInterval) new IntFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);
        and1 = (And) FilterFactory.and(ltEq1, gtEq1);
        ans = (IntInterval) new IntFilterVerifier().getInterval(and1);
        assertEquals(ans.count, 0);

        // Or Operator
        gtEq1 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2, false);
        ltEq1 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2, false);
        Or or1 = (Or) FilterFactory.or(gtEq1, ltEq1);
        ans = (IntInterval) new IntFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 4);
        assertEquals(ans.v[0], Integer.MIN_VALUE); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], 2L); assertEquals(ans.flag[1], false);
        assertEquals(ans.v[2], 2L); assertEquals(ans.flag[2], false);
        assertEquals(ans.v[3], Integer.MAX_VALUE); assertEquals(ans.flag[3], true);
        or1 = (Or) FilterFactory.or(ltEq1, gtEq1);
        ans = (IntInterval) new IntFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 4);
        assertEquals(ans.v[0], Integer.MIN_VALUE); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], 2L); assertEquals(ans.flag[1], false);
        assertEquals(ans.v[2], 2L); assertEquals(ans.flag[2], false);
        assertEquals(ans.v[3], Integer.MAX_VALUE); assertEquals(ans.flag[3], true);

        gtEq1 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2, true);
        ltEq1 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2, false);
        or1 = (Or) FilterFactory.or(gtEq1, ltEq1);
        ans = (IntInterval) new IntFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 2);
        assertEquals(ans.v[0], Integer.MIN_VALUE); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], Integer.MAX_VALUE); assertEquals(ans.flag[1], true);
        or1 = (Or) FilterFactory.or(ltEq1, gtEq1);
        ans = (IntInterval) new IntFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 2);
        assertEquals(ans.v[0], Integer.MIN_VALUE); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], Integer.MAX_VALUE); assertEquals(ans.flag[1], true);

        gtEq1 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2, false);
        ltEq1 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 2, true);
        or1 = (Or) FilterFactory.or(gtEq1, ltEq1);
        ans = (IntInterval) new IntFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 2);
        assertEquals(ans.v[0], Integer.MIN_VALUE); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], Integer.MAX_VALUE); assertEquals(ans.flag[1], true);
        or1 = (Or) FilterFactory.or(ltEq1, gtEq1);
        ans = (IntInterval) new IntFilterVerifier().getInterval(or1);
        assertEquals(ans.count, 2);
        assertEquals(ans.v[0], Integer.MIN_VALUE); assertEquals(ans.flag[0], true);
        assertEquals(ans.v[1], Integer.MAX_VALUE); assertEquals(ans.flag[1], true);
    }
}
