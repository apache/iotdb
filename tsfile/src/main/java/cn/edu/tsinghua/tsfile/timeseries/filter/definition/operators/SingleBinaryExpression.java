package cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;

import java.io.Serializable;

/**
 * Definition for binary filter operations single series.
 *
 * @author CGF
 */

public abstract class SingleBinaryExpression extends SingleSeriesFilterExpression implements Serializable {

    private static final long serialVersionUID = 1039585564327602465L;

    protected final SingleSeriesFilterExpression left;
    protected final SingleSeriesFilterExpression right;

    protected SingleBinaryExpression(SingleSeriesFilterExpression left, SingleSeriesFilterExpression right) {
        this.left = left;
        this.right = right;
    }

    public SingleSeriesFilterExpression getLeft() {
        return left;
    }

    public SingleSeriesFilterExpression getRight() {
        return right;
    }

    @Override
    public String toString() {
        return "( " + left + "," + right + " )";
    }

}
