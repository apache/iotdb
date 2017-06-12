package cn.edu.thu.tsfiledb.qp.physical.plan;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.Operator.OperatorType;

/**
 * for test
 */
public class InsertPlan extends PhysicalPlan {
    private long insertTime;
    private String value;
    private Path path;
    //insertType : 1 is bufferwrite insert, 2 is overflow insert
    private int insertType;

    public InsertPlan(int insertType, long insertTime, String value, Path path) {
        super(false, OperatorType.INSERT);
        this.insertType = insertType;
        this.insertTime = insertTime;
        this.value = value;
        this.path = path;
    }

    @Override
    public boolean processNonQuery(QueryProcessExecutor exec) throws ProcessorException{
		insertType = exec.insert(path, insertTime, value);
        return true;
    }

    public long getTime() {
        return insertTime;
    }

    public void setTime(long time) {
        this.insertTime = time;
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

	public int getInsertType() {
		return insertType;
	}

	public void setInsertType(int insertType) {
		this.insertType = insertType;
	}
}
