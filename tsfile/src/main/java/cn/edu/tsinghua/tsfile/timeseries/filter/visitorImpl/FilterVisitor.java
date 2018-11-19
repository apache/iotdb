package cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.*;

/**
 * FilterVisitor is implemented by visitor pattern.
 * Implemented using visitor pattern.
 *
 * A FilterVistor must visit all these methods below, per visitor design pattern.
 * And a FilterExpression just need implements an accept() method.
 *
 * @author CGF
 */
public interface FilterVisitor<R> {

    <T extends Comparable<T>> R visit(Eq<T> eq);

    <T extends Comparable<T>> R visit(NotEq<T> notEq);

    <T extends Comparable<T>> R visit(LtEq<T> ltEq);

    <T extends Comparable<T>> R visit(GtEq<T> gtEq);

    R visit(Not not);

    R visit(And and);

    R visit(Or or);

    R visit(NoFilter noFilter);
}
