package org.apache.iotdb.db.qp.physical.crud;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.db.qp.logical.Operator;


public class InsertPlan extends PhysicalPlan {
	private String deviceId;
	private List<String> measurements;
	private List<String> values;
    private long time;

    // insertType
    // 1 : BufferWrite Insert  2 : Overflow Insert
    private int insertType;

    public InsertPlan(String deviceId, long insertTime, List<String> measurementList, List<String> insertValues) {
        super(false, Operator.OperatorType.INSERT);
        this.time = insertTime;
        this.deviceId = deviceId;
        this.measurements = measurementList;
        this.values = insertValues;
    }

    public InsertPlan(int insertType, String deviceId, long insertTime, List<String> measurementList, List<String> insertValues) {
        super(false, Operator.OperatorType.INSERT);
        this.insertType = insertType;
        this.time = insertTime;
        this.deviceId = deviceId;
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
        	ret.add(new Path(deviceId + "." + m));
        }
        return ret;
    }

	public int getInsertType() {
		return insertType;
	}

	public void setInsertType(int insertType) {
		this.insertType = insertType;
	}

	public String getDeviceId() {
        return this.deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InsertPlan that = (InsertPlan) o;
        return time == that.time &&
                Objects.equals(deviceId, that.deviceId) &&
                Objects.equals(measurements, that.measurements) &&
                Objects.equals(values, that.values);
    }

}
