package cn.edu.thu.tsfiledb.qp.physical.crud;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;


public class DeletePlan extends PhysicalPlan {
    private long deleteTime;
    private Path path;

    public DeletePlan() {
        super(false, OperatorType.DELETE);
    }

    public DeletePlan(long deleteTime, Path path) {
        super(false, OperatorType.DELETE);
        this.setDeleteTime(deleteTime);
        this.setPath(path);
    }

    @Override
    public boolean processNonQuery(QueryProcessExecutor executor) throws ProcessorException {
        return executor.delete(path, deleteTime);
    }

    public long getDeleteTime() {
        return deleteTime;
    }

    public void setDeleteTime(long delTime) {
        this.deleteTime = delTime;
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
