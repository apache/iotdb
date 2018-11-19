package cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor;


import cn.edu.tsinghua.tsfile.timeseries.filterV2.operator.*;

public interface AbstractFilterVisitor<R> {

    <T extends Comparable<T>> R visit(Eq<T> eq);

    <T extends Comparable<T>> R visit(NotEq<T> notEq);

    <T extends Comparable<T>> R visit(LtEq<T> ltEq);

    <T extends Comparable<T>> R visit(GtEq<T> gtEq);

    <T extends Comparable<T>> R visit(Gt<T> gt);

    <T extends Comparable<T>> R visit(Lt<T> lt);

    <T extends Comparable<T>> R visit(Not<T> not);

    <T extends Comparable<T>> R visit(And<T> and);

    <T extends Comparable<T>> R visit(Or<T> or);

    <T extends Comparable<T>> R visit(NoRestriction<T> noFilter);
}
