package cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.FilterVisitor;

import java.io.Serializable;

/**
 * Not necessary. Use InvertExpressionVisitor
 *
 * @author CGF
 */
public class Not extends SingleSeriesFilterExpression implements Serializable {

    private static final long serialVersionUID = 584860326604020881L;
    private SingleSeriesFilterExpression that;

    public Not(SingleSeriesFilterExpression that) {
        this.that = that;
    }

    @Override
    public <T> T accept(FilterVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public SingleSeriesFilterExpression getFilterExpression() {
        return this.that;
    }

    @Override
    public String toString() {
        return "Not: " + that;
    }

    @Override
    public FilterSeries<?> getFilterSeries() {
        return that.getFilterSeries();
    }
}
