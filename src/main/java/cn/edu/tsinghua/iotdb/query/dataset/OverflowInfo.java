package cn.edu.tsinghua.iotdb.query.dataset;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

/**
 * Never used
 */
public class OverflowInfo {
    public DynamicOneColumnData insertTrue;
    public DynamicOneColumnData updateTrue;
    public DynamicOneColumnData updateFalse;
    public SingleSeriesFilterExpression timeFilter;

    public OverflowInfo() {

    }

    public OverflowInfo(DynamicOneColumnData insertTrue, DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, SingleSeriesFilterExpression timeFilter) {
        this.insertTrue = insertTrue;
        this.updateTrue = updateTrue;
        this.updateFalse = updateFalse;
        this.timeFilter = timeFilter;
    }

    public OverflowInfo insertTrue(DynamicOneColumnData insertTrue) {
        this.insertTrue = insertTrue;
        return this;
    }

    public OverflowInfo updateTrue(DynamicOneColumnData updateTrue) {
        this.updateTrue = updateTrue;
        return this;
    }

    public OverflowInfo updateFalse(DynamicOneColumnData updateFalse) {
        this.updateFalse = updateFalse;
        return this;
    }

    public OverflowInfo timeFilter(SingleSeriesFilterExpression timeFilter) {
        this.timeFilter = timeFilter;
        return this;
    }
}
