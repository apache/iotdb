package cn.edu.thu.tsfiledb.qp.logical.operator.crud;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.exception.TSTransformException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.physical.plan.InsertPlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;

/**
 * this class extends {@code RootOperator} and process insert statement
 * 
 * @author kangrong
 *
 */
public class InsertOperator extends SFWOperator {
    private static final Logger LOG = LoggerFactory.getLogger(InsertOperator.class);
    private long insertTime;
    private String insertValue;

    public InsertOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.INSERT;
    }


    public void setInsertValue(String value) {
        insertValue = value;
    }

    public void setInsertTime(long time) {
        insertTime = Long.valueOf(time);
    }

    @Override
    public PhysicalPlan transformToPhysicalPlan(QueryProcessExecutor conf)
            throws QueryProcessorException {
        InsertPlan insertPlan = new InsertPlan();
        insertPlan.setTime(insertTime);
        // parse value
        insertPlan.setValue(insertValue);
        // parse path
        List<Path> paths = getSelSeriesPaths(conf);
        if (paths.size() != 1) {
            throw new TSTransformException("for insert command, cannot specified more than one path:{}"+ paths);
        }
        insertPlan.setPath(paths.get(0));
        return insertPlan;
    }

}
