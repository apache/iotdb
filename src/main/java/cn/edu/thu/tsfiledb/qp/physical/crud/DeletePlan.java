package cn.edu.thu.tsfiledb.qp.physical.crud;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;

/**
 * @author kangrong
 * @author qiaojialin
 */
public class DeletePlan extends PhysicalPlan {
    private long deleteTime;
    private List<Path> paths = new ArrayList<>();

    public DeletePlan() {
        super(false, OperatorType.DELETE);
    }

    public DeletePlan(long deleteTime, Path path) {
        super(false, OperatorType.DELETE);
        this.deleteTime = deleteTime;
        this.paths.add(path);
    }

    public DeletePlan(long deleteTime, List<Path> paths) {
        super(false, OperatorType.DELETE);
        this.deleteTime = deleteTime;
        this.paths = paths;
    }

    public long getDeleteTime() {
        return deleteTime;
    }

    public void setDeleteTime(long delTime) {
        this.deleteTime = delTime;
    }

    public void addPath(Path path) {
        this.paths.add(path);
    }

    public void setPaths(List<Path> paths) {
        this.paths = paths;
    }

    @Override
    public List<Path> getPaths() {
        return paths;
    }

}
