package cn.edu.tsinghua.tsfile.timeseries.filterV2.operator;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.AbstractFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.TimeValuePairFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

import java.io.Serializable;

/**
 * Not necessary. Use InvertExpressionVisitor
 *
 * @author CGF
 */
public class Not<T extends Comparable<T>> implements Filter<T>, Serializable {

    private static final long serialVersionUID = 584860326604020881L;
    private Filter that;

    public Not(Filter that) {
        this.that = that;
    }

    @Override
    public <R> R accept(AbstractFilterVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public <R> R accept(TimeValuePair value, TimeValuePairFilterVisitor<R> visitor) {
        return visitor.visit(value, this);
    }

    public Filter getFilterExpression() {
        return this.that;
    }

    @Override
    public String toString() {
        return "Not: " + that;
    }

}
