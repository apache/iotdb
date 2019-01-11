package cn.edu.tsinghua.tsfile.read.expression.impl;

import cn.edu.tsinghua.tsfile.read.expression.ExpressionType;
import cn.edu.tsinghua.tsfile.read.expression.IUnaryExpression;
import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.read.common.Path;


public class SingleSeriesExpression implements IUnaryExpression {
    private Path seriesPath;
    private Filter filter;

    public SingleSeriesExpression(Path seriesDescriptor, Filter filter) {
        this.seriesPath = seriesDescriptor;
        this.filter = filter;
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.SERIES;
    }

    @Override
    public Filter getFilter() {
        return filter;
    }

    @Override
    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    @Override
    public String toString() {
        return "[" + seriesPath + ":" + filter + "]";
    }

    public Path getSeriesPath() {
        return this.seriesPath;
    }
}
