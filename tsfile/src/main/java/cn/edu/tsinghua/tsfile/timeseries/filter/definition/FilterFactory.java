package cn.edu.tsinghua.tsfile.timeseries.filter.definition;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.*;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.*;

/**
 * The FilterFactory is used to construct FilterSeries, SingleSeriesFilter, and
 * CrossSeriesFilter.
 *
 * @author CGF
 */
public final class FilterFactory {

    /**
     * To construct Time FilterSeries
     *
     * @return LongFilterSeries
     */
    public static LongFilterSeries timeFilterSeries() {
        return new LongFilterSeries(null, null, TSDataType.INT64, FilterSeriesType.TIME_FILTER);
    }

    /**
     * To construct IntFilterSeries
     *
     * @param deltaObjectUID delta object ID
     * @param measurementUID measurement ID
     * @param filterType filter type
     * @return IntFilterSeries
     */
    public static IntFilterSeries intFilterSeries(String deltaObjectUID, String measurementUID,
                                                  FilterSeriesType filterType) {
        return new IntFilterSeries(deltaObjectUID, measurementUID, TSDataType.INT32, filterType);
    }

    /**
     * To construct DoubleFilterSeries
     *
     * @param deltaObjectUID delta object ID
     * @param measurementUID measurement ID
     * @param filterType filter type
     * @return DoubleFilterSeries
     */
    public static DoubleFilterSeries doubleFilterSeries(String deltaObjectUID, String measurementUID,
                                                        FilterSeriesType filterType) {
        return new DoubleFilterSeries(deltaObjectUID, measurementUID, TSDataType.DOUBLE, filterType);
    }

    /**
     * To construct LongFilterSeries
     *
     * @param deltaObjectUID delta object ID
     * @param measurementUID measurement ID
     * @param filterType filter type
     * @return LongFilterSeries
     */
    public static LongFilterSeries longFilterSeries(String deltaObjectUID, String measurementUID,
                                                    FilterSeriesType filterType) {
        return new LongFilterSeries(deltaObjectUID, measurementUID, TSDataType.INT64, filterType);
    }

    /**
     * To construct FloatFilterSeries
     *
     * @param deltaObjectUID delta object ID
     * @param measurementUID measurement ID
     * @param filterType filter type
     * @return FloatFilterSeries
     */
    public static FloatFilterSeries floatFilterSeries(String deltaObjectUID, String measurementUID,
                                                      FilterSeriesType filterType) {
        return new FloatFilterSeries(deltaObjectUID, measurementUID, TSDataType.FLOAT, filterType);
    }

    /**
     * To construct BooleanFilterSeries
     *
     * @param deltaObjectUID delta object ID
     * @param measurementUID measurement ID
     * @param filterType filter type
     * @return BooleanFilterSeries
     */
    public static BooleanFilterSeries booleanFilterSeries(String deltaObjectUID, String measurementUID,
                                                          FilterSeriesType filterType) {
        return new BooleanFilterSeries(deltaObjectUID, measurementUID, TSDataType.BOOLEAN, filterType);
    }

    /**
     * To construct StringFilterSeries
     *
     * @param deltaObjectUID delta object ID
     * @param measurementUID measurement ID
     * @param filterType filter type
     * @return StringFilterSeries
     */
    public static StringFilterSeries stringFilterSeries(String deltaObjectUID, String measurementUID,
                                                        FilterSeriesType filterType) {
        return new StringFilterSeries(deltaObjectUID, measurementUID, TSDataType.TEXT, filterType);
    }

    /**
     * To generate Eq by filterSeries
     *
     * @param filterSeries filter series
     * @param value filter value
     * @param <T> comparable data type
     * @param <C> subclass of FilterSeries
     * @return filter
     */
    public static <T extends Comparable<T>, C extends FilterSeries<T>> Eq<T> eq(C filterSeries, T value) {
        return new Eq<T>(filterSeries, value);
    }

    /**
     * To generate LtEq by filterSeries
     *
     * @param filterSeries filter series
     * @param value filter value
     * @param ifEq if equal
     * @param <T> comparable data type
     * @param <C> subclass of FilterSeries
     * @return lt expression
     */
    public static <T extends Comparable<T>, C extends FilterSeries<T>> LtEq<T> ltEq(C filterSeries, T value,
                                                                                    Boolean ifEq) {
        return new LtEq<T>(filterSeries, value, ifEq);
    }

    /**
     * To generate GtEq by filterSeries
     *
     * @param filterSeries filter series
     * @param value filter value
     * @param ifEq if equal
     * @param <T> comparable data type
     * @param <C> subclass of FilterSeries
     * @return gt expression
     */
    public static <T extends Comparable<T>, C extends FilterSeries<T>> GtEq<T> gtEq(C filterSeries, T value,
                                                                                    Boolean ifEq) {
        return new GtEq<T>(filterSeries, value, ifEq);
    }

    /**
     * To generate NotEq by filterSeries
     *
     * @param filterSeries filter series
     * @param value filter value
     * @param <T> comparable data type
     * @param <C> subclass of FilterSeries
     * @return not equal expression
     */
    public static <T extends Comparable<T>, C extends FilterSeries<T>> NotEq<T> noteq(C filterSeries, T value) {
        return new NotEq<T>(filterSeries, value);
    }

    /**
     * To generate Not by filterSeries
     *
     * @param that not expression
     * @return not expression
     */
    public static SingleSeriesFilterExpression not(SingleSeriesFilterExpression that) {
        return new Not(that);
    }

    /**
     * To generate And by filterSeries
     *
     * @param left left expression
     * @param right right expression
     * @return and expression
     */
    private static SingleSeriesFilterExpression ssAnd(SingleSeriesFilterExpression left, SingleSeriesFilterExpression right) {
//		if (left.getFilterSeries().sameSeries(right.getFilterSeries()))
        return new And(left, right);
//		else
//			return new CSAnd(left, right);
    }

    /**
     * To generate Or by filterSeries
     *
     * @param left left expression
     * @param right right expression
     * @return or expression
     */
    private static SingleSeriesFilterExpression ssOr(SingleSeriesFilterExpression left, SingleSeriesFilterExpression right) {
//		if (left.getFilterSeries().sameSeries(right.getFilterSeries()))
        return new Or(left, right);
//		else
//			return new CSOr(left, right);
    }

    /**
     * construct CSAnd(Cross Series Filter And Operators) use FilterExpression
     * left, right;
     *
     * @param left left expression
     * @param right right expression
     * @return and expression
     */
    public static CSAnd csAnd(FilterExpression left, FilterExpression right) {
        return new CSAnd(left, right);
    }

    /**
     * construct CSOr(Cross Series Filter Or Operators) use FilterExpression
     * left, right;
     *
     * @param left left expression
     * @param right right expression
     * @return or expression
     */
    public static CSOr csOr(FilterExpression left, FilterExpression right) {
        return new CSOr(left, right);
    }

    public static FilterExpression and(FilterExpression left, FilterExpression right) {
        if (left.getFilterSeries().getFilterType() == FilterSeriesType.TIME_FILTER &&
                right.getFilterSeries().getFilterType() == FilterSeriesType.TIME_FILTER) {
            return ssAnd((SingleSeriesFilterExpression) left, (SingleSeriesFilterExpression) right);
        }

        if (left instanceof SingleSeriesFilterExpression && right instanceof SingleSeriesFilterExpression
                && (((SingleSeriesFilterExpression) left).getFilterSeries().sameSeries(((SingleSeriesFilterExpression) right).getFilterSeries()))) {
            return ssAnd((SingleSeriesFilterExpression) left, (SingleSeriesFilterExpression) right);
        } else {
            return csAnd(left, right);
        }
    }

    public static FilterExpression or(FilterExpression left, FilterExpression right) {
        if (left.getFilterSeries().getFilterType() == FilterSeriesType.TIME_FILTER &&
                right.getFilterSeries().getFilterType() == FilterSeriesType.TIME_FILTER) {
            return ssOr((SingleSeriesFilterExpression) left, (SingleSeriesFilterExpression) right);
        }

        if (left instanceof SingleSeriesFilterExpression && right instanceof SingleSeriesFilterExpression
                && (((SingleSeriesFilterExpression) left).getFilterSeries().sameSeries(((SingleSeriesFilterExpression) right).getFilterSeries()))) {
            return ssOr((SingleSeriesFilterExpression) left, (SingleSeriesFilterExpression) right);
        } else {
            return csOr(left, right);
        }
    }
}
