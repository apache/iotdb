package cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.*;

/**
 * To remove not operators, convert all operators recursively.
 * {@code Not(and(eq(), not(eq(y))) -> Or(notEq(), eq(y))}
 *
 * @author CGF
 */
public class ConvertExpressionVisitor implements FilterVisitor<FilterExpression> {

    private InvertExpressionVisitor invertor = new InvertExpressionVisitor();

    public FilterExpression convert(FilterExpression exp) {
        return exp.accept(this);
    }

    @Override
    public <T extends Comparable<T>> FilterExpression visit(Eq<T> eq) {
        return eq;
    }

    @Override
    public <T extends Comparable<T>> FilterExpression visit(NotEq<T> notEq) {
        return notEq;
    }

    @Override
    public <T extends Comparable<T>> FilterExpression visit(LtEq<T> ltEq) {
        return ltEq;
    }

    @Override
    public <T extends Comparable<T>> FilterExpression visit(GtEq<T> gtEq) {
        return gtEq;
    }


    @Override
    public FilterExpression visit(And and) {
        return FilterFactory.and((and.getLeft()), convert(and.getRight()));
    }

    @Override
    public FilterExpression visit(Or or) {
        return FilterFactory.or(convert(or.getLeft()), convert(or.getRight()));
    }

    @Override
    public FilterExpression visit(NoFilter noFilter) {
        return noFilter;
    }

    @Override
    public FilterExpression visit(Not not) {
        return invertor.invert(not.getFilterExpression());
    }
}

