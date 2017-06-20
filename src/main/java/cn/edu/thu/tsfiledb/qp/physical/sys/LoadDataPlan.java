package cn.edu.thu.tsfiledb.qp.physical.sys;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;

/**
 * @author kangrong
 * @author qiaojialin
 */
public class LoadDataPlan extends PhysicalPlan {
    private final String inputFilePath;
    private final String measureType;

    public LoadDataPlan(String inputFilePath, String measureType) {
        super(false, OperatorType.LOADDATA);
        this.inputFilePath = inputFilePath;
        this.measureType = measureType;
    }

    @Override
    public List<Path> getPaths() {
        List<Path> ret = new ArrayList<>();
        if (measureType != null)
            ret.add(new Path(measureType));
        return ret;
    }

    public String getInputFilePath() {
        return inputFilePath;
    }

    public String getMeasureType() {
        return measureType;
    }
}
