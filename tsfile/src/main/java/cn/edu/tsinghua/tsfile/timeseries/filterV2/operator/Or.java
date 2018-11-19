package cn.edu.tsinghua.tsfile.timeseries.filterV2.operator;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.BinaryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.AbstractFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.TimeValuePairFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

import java.io.Serializable;

/**
 * Either of the left and right operators of And must satisfy the condition.
 *
 * @author CGF
 */
public class Or<T extends Comparable<T>> extends BinaryFilter<T> implements Serializable {

    private static final long serialVersionUID = -968055896528472694L;

    public Or(Filter left, Filter right) {
        super(left, right);
    }

    @Override
    public String toString() {
        return "(" + left + " || " + right + ")";
    }

    @Override
    public <R> R accept(AbstractFilterVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public <R> R accept(TimeValuePair value, TimeValuePairFilterVisitor<R> visitor) {
        return visitor.visit(value, this);
    }
}
