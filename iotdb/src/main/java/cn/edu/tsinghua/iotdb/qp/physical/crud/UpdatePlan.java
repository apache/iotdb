package cn.edu.tsinghua.iotdb.qp.physical.crud;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.utils.Pair;
import cn.edu.tsinghua.tsfile.utils.StringContainer;

import static cn.edu.tsinghua.iotdb.qp.constant.SQLConstant.lineFeedSignal;


public class UpdatePlan extends PhysicalPlan {
	private List<Pair<Long, Long>> intervals = new ArrayList<>();
    private String value;
    private Path path;

    public UpdatePlan() {
        super(false, Operator.OperatorType.UPDATE);
    }

    public UpdatePlan(long startTime, long endTime, String value, Path path) {
        super(false, Operator.OperatorType.UPDATE);
        setValue(value);
        setPath(path);
        addInterval(new Pair<>(startTime, endTime));
    }

    public UpdatePlan(List<Pair<Long,Long>> list, String value, Path path) {
        super(false, Operator.OperatorType.UPDATE);
        setValue(value);
        setPath(path);
        intervals = list;
    }

    public List<Pair<Long, Long>> getIntervals() {
        return intervals;
    }

    public void addInterval(Pair<Long, Long> interval) {
        this.intervals.add(interval);
    }

    public void addIntervals(List<Pair<Long, Long>> intervals) {
        this.intervals.addAll(intervals);
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }
    
    @Override
    public List<Path> getPaths() {
        List<Path> ret = new ArrayList<>();
        if (path != null) {
            ret.add(path);
        }
        return ret;
    }

    @Override
    public String printQueryPlan() {
        StringContainer sc = new StringContainer();
        String preSpace = "  ";
        sc.addTail("UpdatePlan:");
        sc.addTail(preSpace, "paths:  ", path.toString(), lineFeedSignal);
        sc.addTail(preSpace, "value:", value, lineFeedSignal);
        sc.addTail(preSpace, "filter: ", lineFeedSignal);
        intervals.forEach(p->sc.addTail(preSpace, preSpace, p.left,p.right, lineFeedSignal));
        return sc.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UpdatePlan that = (UpdatePlan) o;
        return Objects.equals(intervals, that.intervals) &&
                Objects.equals(value, that.value) &&
                Objects.equals(path, that.path);
    }
}
