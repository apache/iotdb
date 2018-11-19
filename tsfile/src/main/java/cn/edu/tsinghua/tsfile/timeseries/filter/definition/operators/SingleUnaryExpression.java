package cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators;

import cn.edu.tsinghua.tsfile.common.exception.FilterInvokeException;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.FilterVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Definition for unary filter operations of single series.
 *
 * @param <T> comparable data type
 * @author CGF
 */
public class SingleUnaryExpression<T extends Comparable<T>> extends SingleSeriesFilterExpression
        implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(SingleUnaryExpression.class);
    private static final long serialVersionUID = 1431606024929453556L;
    protected final FilterSeries<T> filterSeries;
    protected final T value;

    protected SingleUnaryExpression(FilterSeries<T> filterSeries, T value) {
        this.filterSeries = filterSeries;
        this.value = value;
    }

    public FilterSeries<T> getFilterSeries() {
        return filterSeries;
    }

    public T getValue() {
        return value;
    }

    @Override
    public String toString() {
        return filterSeries + " - " + value;
    }

    @SuppressWarnings("hiding")
    @Override
    public <T> T accept(FilterVisitor<T> visitor) {
        // Never be invoked
        // This method is invoked by specific UnarySeriesFilter which is
        // subclass of UnarySeriesFilter,
        // such as LtEq, Eq..
        LOG.error("UnarySeriesFilter's accept method can never be invoked.");
        throw new FilterInvokeException("UnarySeriesFilter's accept method can never be invoked.");
    }
}
