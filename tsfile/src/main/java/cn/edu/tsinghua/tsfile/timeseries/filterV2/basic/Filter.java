package cn.edu.tsinghua.tsfile.timeseries.filterV2.basic;


import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.AbstractFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.TimeValuePairFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

/**
 * Filter is a top level filter abstraction.
 * Filter has two types of implementations : {@link BinaryFilter} and
 * {@link UnaryFilter}
 * Filter is a role of interviewee in visitor pattern.
 *
 * @author CGF
 */
public interface Filter<T extends Comparable<T>> {

    <R> R accept(AbstractFilterVisitor<R> visitor);

    <R> R accept(TimeValuePair timeValuePair, TimeValuePairFilterVisitor<R> visitor);

}
