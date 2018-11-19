package cn.edu.tsinghua.tsfile.timeseries.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.IntFilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.SingleUnaryExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.InvertExpressionVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Or;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author CGF
 *
 */
public class InvertExpressionVisitorTest {
    private static final Logger LOG = LoggerFactory.getLogger(InvertExpressionVisitorTest.class);
	private static String deltaObjectINT = FilterTestConstant.deltaObjectINT;
	private static String measurementINT = FilterTestConstant.measurementINT;
	
    private static final IntFilterSeries intFilterSeries =
            FilterFactory.intFilterSeries(deltaObjectINT, measurementINT, FilterSeriesType.VALUE_FILTER);

    private static final InvertExpressionVisitor invertor = new InvertExpressionVisitor();

    SingleSeriesFilterExpression ltEq = FilterFactory.ltEq(intFilterSeries, 60, true);
    SingleSeriesFilterExpression gEq = FilterFactory.gtEq(intFilterSeries, 30, false);
    SingleSeriesFilterExpression eq = FilterFactory.eq(intFilterSeries, 60);

 
    @Test 
    public void testInvertUnaryOperator() {

        // ltEq(60, true) -> gEq(60, false);
        FilterExpression notLtEq = invertor.invert(ltEq);
        assertEquals(notLtEq.toString(),
                "FilterSeries (" + deltaObjectINT + "," + measurementINT + ",INT32,VALUE_FILTER) > 60");


        // gEq(30, false) -> ltEq(30, true);
        FilterExpression notGEq = invertor.invert(gEq);
        // LOG.info(notGEq.toString());
        assertEquals(notGEq.toString(),
                "FilterSeries (" + deltaObjectINT + "," + measurementINT + ",INT32,VALUE_FILTER) <= 30");


        // Eq(60) -> notEq(60);
        FilterExpression noteq = invertor.invert(eq);
        // LOG.info(noteq.toString());
        assertEquals(noteq.toString(),
                "FilterSeries (" + deltaObjectINT +  "," + measurementINT + ",INT32,VALUE_FILTER) != 60");

    }

    @Test
    public void testInvertBinaryOperator() {

        // AND(ltEq(60, true), gEq(30, false)) -> OR[gtEq(60, false), ltEq(30, true)]
        FilterExpression and = FilterFactory.and(ltEq, gEq);
        FilterExpression andInvert = invertor.invert(and);
        assertEquals(andInvert.toString(),
                "OR: ( FilterSeries (" + deltaObjectINT + "," + measurementINT + ",INT32,VALUE_FILTER) > 60,"
                        + "FilterSeries (" + deltaObjectINT + "," + measurementINT +",INT32,VALUE_FILTER) <= 30 )");


        // OR(eq(60), not(ltEq(60,true)) -> AND(notEq(60), ltEq(60,true));
        FilterExpression or = FilterFactory.or(eq, FilterFactory.not(ltEq));
        FilterExpression orInvert = invertor.invert(or);
        assertEquals(orInvert.toString(),
                "AND: ( FilterSeries (" + deltaObjectINT + "," + measurementINT + ",INT32,VALUE_FILTER) != 60,"
                        + "FilterSeries (" + deltaObjectINT +"," + measurementINT + ",INT32,VALUE_FILTER) <= 60 )");


        // { 20, [300,500), (500, 6000) } ->
        SingleSeriesFilterExpression eqQ = FilterFactory.eq(intFilterSeries, 20);
        SingleSeriesFilterExpression ltEqQ = FilterFactory.ltEq(intFilterSeries, 6000, false);
        SingleSeriesFilterExpression gtEqQ = FilterFactory.gtEq(intFilterSeries, 300, true);
        SingleSeriesFilterExpression notEqQ = FilterFactory.noteq(intFilterSeries, 500);
        FilterExpression complex = FilterFactory.and(eqQ, FilterFactory.and(ltEqQ, FilterFactory.and(gtEqQ, notEqQ)));
        FilterExpression complexInvert = invertor.invert(complex);
        LOG.info(complexInvert.toString());
        assertTrue(complexInvert instanceof Or);
        assertTrue(((Or)complexInvert).getRight() instanceof Or);
        SingleUnaryExpression<?> e = (SingleUnaryExpression<?>) ((Or)complexInvert).getLeft();
        assertTrue((Integer)e.getValue() == 20);

    }
    

}
