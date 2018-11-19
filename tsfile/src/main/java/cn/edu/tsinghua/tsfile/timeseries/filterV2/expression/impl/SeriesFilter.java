package cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilterType;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.UnaryQueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.TimeValuePairFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.impl.TimeValuePairFilterVisitorImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

/**
 * Created by zhangjinrui on 2017/12/15.
 */
public class SeriesFilter<T extends Comparable<T>> implements UnaryQueryFilter {
    private TimeValuePairFilterVisitor<Boolean> timeValuePairFilterVisitor;
    private Path seriesPath;
    private Filter<T> filter;

    public SeriesFilter(Path seriesDescriptor, Filter<T> filter) {
        this.seriesPath = seriesDescriptor;
        this.filter = filter;
        timeValuePairFilterVisitor = new TimeValuePairFilterVisitorImpl();
    }

    public boolean satisfy(TimeValuePair timeValuePair) {
        return timeValuePairFilterVisitor.satisfy(timeValuePair, this.filter);
    }

    @Override
    public QueryFilterType getType() {
        return QueryFilterType.SERIES;
    }

    public Filter<T> getFilter() {
        return filter;
    }

    public void setFilter(Filter<T> filter) {
        this.filter = filter;
    }

    public String toString() {
        return "[" + seriesPath + ":" + filter + "]";
    }

    public Path getSeriesPath() {
        return this.seriesPath;
    }
}
