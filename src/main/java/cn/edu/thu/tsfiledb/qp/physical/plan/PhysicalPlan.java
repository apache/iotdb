package cn.edu.thu.tsfiledb.qp.physical.plan;

import java.util.Iterator;
import java.util.List;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.qp.exception.physical.plan.PhysicalPlanException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.Operator.OperatorType;

/**
 * This class is a abstract class for all type of PhysicalPlan. PhysicalPlan is a binary tree and is
 * processed along preorder traversal.
 * 
 * @author kangrong
 *
 */
public abstract class PhysicalPlan {
    private boolean isQuery;
    private OperatorType operatorType;
    private Path path;

    protected PhysicalPlan(boolean isQuery, OperatorType operatorType) {
        this.isQuery = isQuery;
        this.operatorType = operatorType;
    }

    /**
     * input a getIndex processing config and process the getIndex
     * 
     * @param config
     * @return
     */
    public Iterator<QueryDataSet> processQuery(QueryProcessExecutor config) {
        throw new UnsupportedOperationException();
    }

    /**
     * input a getIndex processing config and process insert/update/delete
     * 
     * @param config
     * @return
     * @throws PhysicalPlanException
     */
    public boolean processNonQuery(QueryProcessExecutor config) throws ProcessorException {
        throw new UnsupportedOperationException();
    }

    public String printQueryPlan() {
        return "";
    }

    public abstract List<Path> getInvolvedSeriesPaths();

    public boolean isQuery(){
        return isQuery;
    }

    public OperatorType getOperatorType() {
        return operatorType;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }
}
