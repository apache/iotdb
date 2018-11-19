package cn.edu.tsinghua.tsfile.timeseries.filterV2.operator;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.BinaryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.AbstractFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.TimeValuePairFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

/**
 * Both the left and right operators of And must satisfy the condition.
 *
 * @author CGF
 */
public class And<T extends Comparable<T>> extends BinaryFilter<T> {

    private static final long serialVersionUID = 6705254093824897938L;

    public And(Filter left, Filter right) {
        super(left, right);
    }

    @Override
    public <R> R accept(AbstractFilterVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public <R> R accept(TimeValuePair value, TimeValuePairFilterVisitor<R> visitor) {
        return visitor.visit(value, this);
    }

    @Override
    public String toString() {
        return "(" + left + " && " + right + ")";
    }
}
