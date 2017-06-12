package cn.edu.thu.tsfiledb.qp.physical.sys;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.thu.tsfiledb.utils.LoadDataUtils;


public class LoadDataPlan extends PhysicalPlan {
    private final String inputFilePath;
    private final String measureType;

    public LoadDataPlan(String inputFilePath, String measureType) {
        super(false, OperatorType.LOADDATA);
        this.inputFilePath = inputFilePath;
        this.measureType = measureType;
    }

    public boolean processNonQuery(QueryProcessExecutor config) {
        LoadDataUtils load = new LoadDataUtils();
        load.loadLocalDataMultiPass(inputFilePath, measureType, config.getMManager());
        return true;
    }

    @Override
    public List<Path> getPaths() {
        List<Path> ret = new ArrayList<>();
        if (measureType != null)
            ret.add(new Path(measureType));
        return ret;
    }
}
