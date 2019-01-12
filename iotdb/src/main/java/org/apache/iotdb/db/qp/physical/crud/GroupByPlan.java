package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.db.qp.logical.Operator;

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
