package cn.edu.tsinghua.iotdb.qp.physical.sys;

import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.tsfile.read.common.Path;

public class LoadDataPlan extends PhysicalPlan {
    private final String inputFilePath;
    private final String measureType;

    public LoadDataPlan(String inputFilePath, String measureType) {
        super(false, Operator.OperatorType.LOADDATA);
        this.inputFilePath = inputFilePath;
        this.measureType = measureType;
    }

    @Override
    public List<Path> getPaths() {
        List<Path> ret = new ArrayList<>();
        if (measureType != null) {
            ret.add(new Path(measureType));
        }
        return ret;
    }

    public String getInputFilePath() {
        return inputFilePath;
    }

    public String getMeasureType() {
        return measureType;
    }
}
