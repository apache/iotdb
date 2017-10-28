package cn.edu.tsinghua.iotdb.qp.physical;

import java.util.List;

import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;

/**
 * This class is a abstract class for all type of PhysicalPlan.
 * 
 * @author kangrong
 * @author qiaojialin
 */
public abstract class PhysicalPlan {
    private boolean isQuery;
    private Operator.OperatorType operatorType;

    protected PhysicalPlan(boolean isQuery, Operator.OperatorType operatorType) {
        this.isQuery = isQuery;
        this.operatorType = operatorType;
    }

    public void setOperatorType(Operator.OperatorType operatorType) {
        this.operatorType = operatorType;
    }

    public String printQueryPlan() {
        return "abstract plan";
    }

    public abstract List<Path> getPaths();

    public boolean isQuery(){
        return isQuery;
    }

    public Operator.OperatorType getOperatorType() {
        return operatorType;
    }

    public List<String> getAggregations(){
        return null;
    }
}
