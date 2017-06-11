package cn.edu.thu.tsfiledb.qp.logical.operator.root.sfw;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.exception.TSTransformException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
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
    private List<String> valueList;
    
    
    public MultiInsertOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.MULTIINSERT;
    }

    public void setInsertTime(long time) {
        insertTime = time;
    }
    

    @Override
    public PhysicalPlan transformToPhysicalPlan(QueryProcessExecutor conf)
            throws QueryProcessorException {
    	List<Path> paths = getSelSeriesPaths(conf);
    	if(paths.size() != 1){
    		throw new TSTransformException("for MultiInsert command, cannot specified more than one path:{}"+ paths);
    	}
    	Path deltaObject = paths.get(0);

		return new MultiInsertPlan(deltaObject.getFullPath(), insertTime, measurementList, valueList);
    }

	public void setMeasurementList(List<String> measurementList) {
		this.measurementList = measurementList;
	}

	public void setValueList(List<String> insertValue) {
		this.valueList = insertValue;
	}

	public List<String> getMeasurementList() {
		return measurementList;
	}

	public List<String> getValueList() {
		return valueList;
	}

}
