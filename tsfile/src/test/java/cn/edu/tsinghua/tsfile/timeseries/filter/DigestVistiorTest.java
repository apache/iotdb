package cn.edu.tsinghua.tsfile.timeseries.filter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.And;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.LtEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.NotEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.DigestVisitor;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Eq;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.GtEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Not;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Or;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;

/**
 * 
 * @author CGF
 *
 */
public class DigestVistiorTest {
	private static String deltaObjectUID = FilterTestConstant.deltaObjectUID;
	private static String measurementUID = FilterTestConstant.measurementUID;
	
    private DigestVisitor digestVistor = new DigestVisitor();

    private ByteBuffer b1 = ByteBuffer.wrap(BytesUtils.intToBytes(45));
    private ByteBuffer b2 = ByteBuffer.wrap(BytesUtils.intToBytes(78));
    private DigestForFilter digest1 = new DigestForFilter(b1, b2, TSDataType.INT32); // (45, 78, INT32)
 
    @Test
    public void testIntegerEq() {
        Eq<Integer> eq = FilterFactory.eq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45);
        assertTrue(digestVistor.satisfy(digest1, eq));

        Eq<Integer> eqNot = FilterFactory.eq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 90);
        assertFalse(digestVistor.satisfy(digest1, eqNot));
    }

    @Test
    public void testIntegerNotEq() {
        NotEq<Integer> notEq = FilterFactory.noteq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 60);
        assertTrue(digestVistor.satisfy(digest1, notEq));

        NotEq<Integer> eqNot = FilterFactory.noteq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45);
        assertFalse(digestVistor.satisfy(digest1, eqNot));
    }

    @Test
    public void testIntegerLtEq() {
        LtEq<Integer> ltEq1 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 990, true);
        assertTrue(digestVistor.satisfy(digest1, ltEq1));

        LtEq<Integer> ltEq2 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45, true);
        assertTrue(digestVistor.satisfy(digest1, ltEq2));

        LtEq<Integer> ltEq3 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 60, true);
        assertTrue(digestVistor.satisfy(digest1, ltEq3));

        LtEq<Integer> ltEq4 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 44, true);
        assertFalse(digestVistor.satisfy(digest1, ltEq4));

        LtEq<Integer> ltEq5 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45, false);
        assertFalse(digestVistor.satisfy(digest1, ltEq5));
    }

    @Test
    public void testIntegerGtEq() {
        GtEq<Integer> gtEq1 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 990, true);
        assertFalse(digestVistor.satisfy(digest1, gtEq1));

        GtEq<Integer> gtEq2 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45, true);
        assertTrue(digestVistor.satisfy(digest1, gtEq2));

        GtEq<Integer> gtEq3 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 60, true);
        assertTrue(digestVistor.satisfy(digest1, gtEq3));

        GtEq<Integer> gtEq4 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 44, true);
        assertTrue(digestVistor.satisfy(digest1, gtEq4));

        GtEq<Integer> gtEq5 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45, false);
        assertTrue(digestVistor.satisfy(digest1, gtEq5));

        GtEq<Integer> gtEq6 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 78, false);
        assertFalse(digestVistor.satisfy(digest1, gtEq6));
    }

    @Test
    public void testNot() {
        Not not = (Not) FilterFactory.not(FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 990, true));
        assertTrue(digestVistor.satisfy(digest1, not));
    }

    @Test
    public void testAnd() {
        LtEq<Integer> ltEq1 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 46, true);
        GtEq<Integer> gtEq1 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 77, true);
        And and = (And) FilterFactory.and(ltEq1, gtEq1);
        assertTrue(digestVistor.satisfy(digest1, and));
        
        LtEq<Integer> ltEq2 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 46, true);
        GtEq<Integer> gtEq2 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 79, true);
        And and2 = (And) FilterFactory.and(ltEq2, gtEq2);
        assertFalse(digestVistor.satisfy(digest1, and2));
    }

    @Test
    public void testOr() {
        LtEq<Integer> ltEq1 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 46, true);
        GtEq<Integer> gtEq1 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 77, true);
        Or or = (Or) FilterFactory.or(ltEq1, gtEq1);
        assertTrue(digestVistor.satisfy(digest1, or));
        
        LtEq<Integer> ltEq2 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 33, true);
        GtEq<Integer> gtEq2 = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 79, true);
        Or or2 = (Or) FilterFactory.or(ltEq2, gtEq2);
        assertFalse(digestVistor.satisfy(digest1, or2));
    }
    
    @Test
    public void testNullValue() {
    	// Value is null
    	LtEq<Integer> ltEq = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, 
    			FilterSeriesType.VALUE_FILTER), null, true);
        assertFalse(digestVistor.satisfy(digest1, ltEq));
        
        GtEq<Integer> gtEq = FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, 
    			FilterSeriesType.VALUE_FILTER), null, true);
        assertFalse(digestVistor.satisfy(digest1, gtEq));
        
        Eq<Integer> eq = FilterFactory.eq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, 
    			FilterSeriesType.VALUE_FILTER), null);
        assertFalse(digestVistor.satisfy(digest1, eq));
        
        NotEq<Integer> notEq = FilterFactory.noteq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, 
    			FilterSeriesType.VALUE_FILTER), null);
        assertFalse(digestVistor.satisfy(digest1, notEq));
        
        // FilterSeriesType is null
        LtEq<Integer> ltEq2 = FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObjectUID, measurementUID, 
    			null), 3, true);
        assertFalse(digestVistor.satisfy(digest1, ltEq2));
    }
}
