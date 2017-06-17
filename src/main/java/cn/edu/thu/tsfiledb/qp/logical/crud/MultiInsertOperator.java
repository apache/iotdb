package cn.edu.thu.tsfiledb.qp.logical.crud;

import java.util.List;

/**
 * this class extends {@code RootOperator} and process insert statement
 * 
 * @author kangrong
 *
 */
public class MultiInsertOperator extends SFWOperator {
    private long time;
    private List<String> measurementList;
    private List<String> valueList;
    
    
    public MultiInsertOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.MULTIINSERT;
    }

    public void setTime(long time) {
        this.time = time;
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

	public long getTime() {
    	return time;
	}

}
