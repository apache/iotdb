package cn.edu.tsinghua.iotdb.qp.physical.crud;

import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;

/**
 * @author kangrong
 * @author qiaojialin
 */
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
        if (path != null)
            ret.add(path);
        return ret;
    }
}
