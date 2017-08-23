package cn.edu.thu.tsfiledb.qp.physical.crud;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;

/**
 * @author kangrong
 * @author qiaojialin
 */
public class InsertPlan extends PhysicalPlan {
	private String deltaObject;
	private List<String> measurements;
	private List<String> values;
    private long time;

    // insertType
    // 1 : BufferWrite Insert  2 : Overflow Insert
    private int insertType;

    public InsertPlan(String deltaObject, long insertTime, List<String> measurementList, List<String> insertValues) {
        super(false, OperatorType.INSERT);
        this.time = insertTime;
        this.deltaObject = deltaObject;
        this.measurements = measurementList;
        this.values = insertValues;
    }

    public InsertPlan(int insertType, String deltaObject, long insertTime, List<String> measurementList, List<String> insertValues) {
        super(false, OperatorType.INSERT);
        this.insertType = insertType;
        this.time = insertTime;
        this.deltaObject = deltaObject;
        this.measurements = measurementList;
        this.values = insertValues;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public List<Path> getPaths() {
        List<Path> ret = new ArrayList<>();
        
        for(String m : measurements){
        	ret.add(new Path(deltaObject + "." + m));
        }
        return ret;
    }

	public int getInsertType() {
		return insertType;
	}

	public void setInsertType(int insertType) {
		this.insertType = insertType;
	}

	public String getDeltaObject() {
        return this.deltaObject;
    }

    public void setDeltaObject(String deltaObject) {
        this.deltaObject = deltaObject;
    }

	public List<String> getMeasurements() {
        return this.measurements;
    }

    public void setMeasurements(List<String> measurements) {
        this.measurements = measurements;
    }

    public List<String> getValues() {
        return this.values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }
}
