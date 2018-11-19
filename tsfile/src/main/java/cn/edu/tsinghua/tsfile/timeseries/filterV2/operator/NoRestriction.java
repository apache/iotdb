package cn.edu.tsinghua.tsfile.timeseries.filterV2.operator;


import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.AbstractFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.TimeValuePairFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

/**
 * <code>NoRestriction</code> means that there is no filter.
 */
public class NoRestriction<T extends Comparable<T>> implements Filter<T> {
    private static final NoRestriction INSTANCE = new NoRestriction();

    public static final NoRestriction getInstance() {
        return INSTANCE;
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
