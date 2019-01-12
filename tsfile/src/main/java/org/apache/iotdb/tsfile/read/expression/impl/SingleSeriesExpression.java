package org.apache.iotdb.tsfile.read.expression.impl;

import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IUnaryExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.common.Path;


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
