package cn.edu.thu.tsfiledb.qp.physical.crud;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;

/**
 * @author kangrong
 * @author qiaojialin
 */
public class UpdatePlan extends PhysicalPlan {
	private List<Pair<Long, Long>> intervals = new ArrayList<>();
    private String value;
    private Path path;

    public UpdatePlan() {
        super(false, OperatorType.UPDATE);
    }

    public UpdatePlan(long startTime, long endTime, String value, Path path) {
        super(false, OperatorType.UPDATE);
        setValue(value);
        setPath(path);
        addInterval(new Pair<>(startTime, endTime));
    }

    public UpdatePlan(List<Pair<Long,Long>> list, String value, Path path) {
        super(false, OperatorType.UPDATE);
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
