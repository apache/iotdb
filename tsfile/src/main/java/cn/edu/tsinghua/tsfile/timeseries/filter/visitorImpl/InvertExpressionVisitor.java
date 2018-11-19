package cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.*;

/**
 * Invert(FilterExpression) = Not(FilterExpression)
 * Implemented using visitor pattern.
 *
 * @author CGF
 */
public class InvertExpressionVisitor implements FilterVisitor<FilterExpression> {

    // to invert the expression recursively
    public FilterExpression invert(FilterExpression fe) {
        return fe.accept(this);
    }

    @Override
    public <T extends Comparable<T>> FilterExpression visit(Eq<T> eq) {
        return new NotEq<T>(eq.getFilterSeries(), eq.getValue());
    }

    @Override
    public <T extends Comparable<T>> FilterExpression visit(NotEq<T> notEq) {
        return new Eq<T>(notEq.getFilterSeries(), notEq.getValue());
    }

    @Override
    public <T extends Comparable<T>> FilterExpression visit(LtEq<T> ltEq) {
        if (ltEq.getIfEq()) {
            return FilterFactory.gtEq(ltEq.getFilterSeries(), ltEq.getValue(), false);
        }

        return FilterFactory.gtEq(ltEq.getFilterSeries(), ltEq.getValue(), true);
    }

    @Override
    public <T extends Comparable<T>> FilterExpression visit(GtEq<T> gtEq) {
        if (gtEq.getIfEq()) {
            return FilterFactory.ltEq(gtEq.getFilterSeries(), gtEq.getValue(), false);
        }

        return FilterFactory.ltEq(gtEq.getFilterSeries(), gtEq.getValue(), true);
    }

    @Override
    public FilterExpression visit(And and) {
        return FilterFactory.or(invert(and.getLeft()), invert(and.getRight()));
    }

    @Override
    public FilterExpression visit(Or or) {
        return FilterFactory.and(invert(or.getLeft()), invert(or.getRight()));
    }

    @Override
    public FilterExpression visit(NoFilter noFilter) {
        return null;
    }

    @Override
    public SingleSeriesFilterExpression visit(Not not) {
        return not.getFilterExpression();
    }

}
