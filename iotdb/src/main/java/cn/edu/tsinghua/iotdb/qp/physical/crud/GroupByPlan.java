package cn.edu.tsinghua.iotdb.qp.physical.crud;

import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.tsfile.utils.Pair;

import java.util.List;

public class GroupByPlan extends AggregationPlan{

    private long unit;
    private long origin;
    private List<Pair<Long, Long>> intervals; // show intervals

    public GroupByPlan() {
        super();
        setOperatorType(Operator.OperatorType.GROUPBY);
    }

    public long getUnit() {
        return unit;
    }

    public void setUnit(long unit) {
        this.unit = unit;
    }

    public long getOrigin() {
        return origin;
    }

    public void setOrigin(long origin) {
        this.origin = origin;
    }

    public List<Pair<Long, Long>> getIntervals() {
        return intervals;
    }

    public void setIntervals(List<Pair<Long, Long>> intervals) {
        this.intervals = intervals;
    }
}
