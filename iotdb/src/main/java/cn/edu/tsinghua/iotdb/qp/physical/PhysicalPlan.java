package cn.edu.tsinghua.iotdb.qp.physical;

import java.util.List;

import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.tsfile.read.common.Path;

/**
 * This class is a abstract class for all type of PhysicalPlan.
 */
public abstract class PhysicalPlan {
    private boolean isQuery;
    private Operator.OperatorType operatorType;

    /**
     * The name of the user who proposed this operation.
     */
    private String proposer;

    protected PhysicalPlan(boolean isQuery) {
        this.isQuery = isQuery;
    }

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

    public String getProposer() {
        return proposer;
    }

    public void setProposer(String proposer) {
        this.proposer = proposer;
    }
}
