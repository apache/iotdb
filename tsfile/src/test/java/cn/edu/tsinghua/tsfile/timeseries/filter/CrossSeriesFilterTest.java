package cn.edu.tsinghua.tsfile.timeseries.filter;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.IntFilterSeries;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.DoubleFilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.CSAnd;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.CSOr;

/**
 * 
 * @author CGF
 *
 */
public class CrossSeriesFilterTest {
	private static String deltaObjectINT = FilterTestConstant.deltaObjectINT;
	private static String measurementINT = FilterTestConstant.measurementINT;
	private static String deltaObjectDOUBLE = FilterTestConstant.deltaObjectDOUBLE;
	private static String measurementDOUBLE = FilterTestConstant.measurementDOUBLE;

	private static final IntFilterSeries intFilterSeries = FilterFactory.intFilterSeries(deltaObjectINT, measurementINT,
			FilterSeriesType.VALUE_FILTER);

	private static final DoubleFilterSeries doubleFilterSeries = FilterFactory.doubleFilterSeries(deltaObjectDOUBLE,
			measurementDOUBLE, FilterSeriesType.VALUE_FILTER);

	@Test
	public void testCrossSeriesFilterApi() {
		SingleSeriesFilterExpression left =  FilterFactory.ltEq(intFilterSeries, 60, true);

		SingleSeriesFilterExpression right =  FilterFactory.ltEq(doubleFilterSeries, 60.0, true);

		CSAnd csand = (CSAnd) FilterFactory.and(left, right);
		assertEquals(csand.getLeft(), left);
		assertEquals(csand.getRight(), right);
		assertEquals(csand.toString(),
				"[FilterSeries (deltaObjectINT,measurementINT,INT32,VALUE_FILTER) <= 60] & [FilterSeries (deltaObjectDOUBLE,measurementDOUBLE,DOUBLE,VALUE_FILTER) <= 60.0]");

		CSOr csor = (CSOr) FilterFactory.or(left, right);
		assertEquals(csor.getLeft(), left);
		assertEquals(csor.getRight(), right);
		assertEquals(csor.toString(),
				"[FilterSeries (deltaObjectINT,measurementINT,INT32,VALUE_FILTER) <= 60] | [FilterSeries (deltaObjectDOUBLE,measurementDOUBLE,DOUBLE,VALUE_FILTER) <= 60.0]");
	}
	
	@Test 
	public void XX() {
		
	}
}
