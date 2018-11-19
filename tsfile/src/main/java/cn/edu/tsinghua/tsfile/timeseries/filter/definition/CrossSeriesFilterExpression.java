package cn.edu.tsinghua.tsfile.timeseries.filter.definition;

/**
 *
 * @author CGF
 */
public abstract class CrossSeriesFilterExpression implements FilterExpression {
    protected FilterExpression left;
    protected FilterExpression right;

    protected CrossSeriesFilterExpression(FilterExpression left, FilterExpression right) {
        this.left = left;
        this.right = right;
    }

    public FilterExpression getLeft() {
        return this.left;
    }

    public FilterExpression getRight() {
        return this.right;
    }
}