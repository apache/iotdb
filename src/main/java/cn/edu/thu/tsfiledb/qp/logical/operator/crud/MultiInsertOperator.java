package cn.edu.thu.tsfiledb.qp.logical.operator.crud;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.exception.TSTransformException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.physical.plan.InsertPlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.MultiInsertPlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;

/**
 * this class extends {@code RootOperator} and process insert statement
 * 
 * @author kangrong
 *
 */
public class MultiInsertOperator extends SFWOperator {
    private static final Logger LOG = LoggerFactory.getLogger(MultiInsertOperator.class);
    private long insertTime;
    private List<String> measurementList;
    private List<String> insertValue;
    
    
    public MultiInsertOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.MULTIINSERT;
    }

    public void setInsertTime(long time) {
        insertTime = Long.valueOf(time);
    }
    

    @Override
    public PhysicalPlan transformToPhysicalPlan(QueryProcessExecutor conf)
            throws QueryProcessorException {
    	List<Path> paths = getSelSeriesPaths(conf);
    	if(paths.size() != 1){
    		throw new TSTransformException("for MultiInsert command, cannot specified more than one path:{}"+ paths);
    	}
    	Path deltaObject = paths.get(0);
        MultiInsertPlan multiInsertPlan = new MultiInsertPlan(deltaObject.getFullPath(), insertTime, measurementList, insertValue);
        return multiInsertPlan;
    }

	public List<String> getMeasurementList() {
		return measurementList;
	}

	public void setMeasurementList(List<String> measurementList) {
		this.measurementList = measurementList;
	}

	public List<String> getInsertValue() {
		return insertValue;
	}

	public void setInsertValue(List<String> insertValue) {
		this.insertValue = insertValue;
	}

}
