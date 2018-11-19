package cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.CrossSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.FilterVisitor;

/**
 * Both the left and right operators of CSAnd must satisfy the condition
 * CSAnd represents Cross Series And operation.
 *
 * @author CGF
 */
public class CSAnd extends CrossSeriesFilterExpression {
    public CSAnd(FilterExpression left, FilterExpression right) {
        super(left, right);
    }

    public String toString() {
        return "[" + super.left + "]" + " & [" + super.right + "]";
    }

    /**
     * Not Used
     *
     * @param visitor filter visitor
     * @return accept filter
     */
    @Override
    public <T> T accept(FilterVisitor<T> visitor) {
        return null;
    }

    /**
     * Not Used
     *
     * @return filter
     */
    @Override
    public FilterSeries<?> getFilterSeries() {
        return null;
    }
}
