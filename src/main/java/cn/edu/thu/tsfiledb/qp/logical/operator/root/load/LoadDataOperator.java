package cn.edu.thu.tsfiledb.qp.logical.operator.root.load;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.root.RootOperator;
import cn.edu.thu.tsfiledb.qp.physical.plan.LoadDataPlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;

/**
 * this class maintains information in Author statement, including CREATE, DROP, GRANT and REVOKE
 * 
 * @author kangrong
 *
 */
public class LoadDataOperator extends RootOperator {
    private final String inputFilePath;
    private final String measureType;

    public LoadDataOperator(int tokenIntType, String inputFilePath, String measureType) {
        super(tokenIntType);
        operatorType = OperatorType.LOADDATA;
        this.inputFilePath = inputFilePath;
        this.measureType = measureType;
    }

    @Override
    public PhysicalPlan transformToPhysicalPlan(QueryProcessExecutor conf)
            throws QueryProcessorException {
        return new LoadDataPlan(inputFilePath, measureType);
    }

    public String getInputFilePath() {
        return inputFilePath;
    }

    public String getMeasureType() {
        return measureType;
    }
}
