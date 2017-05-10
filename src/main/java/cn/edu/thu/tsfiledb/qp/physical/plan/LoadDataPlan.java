package cn.edu.thu.tsfiledb.qp.physical.plan;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.Operator.OperatorType;
import cn.edu.thu.tsfiledb.utils.LoadDataUtils;

/**
 * given a author related plan and construct a {@code AuthorPlan}
 * 
 * @author kangrong
 *
 */
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
    public List<Path> getInvolvedSeriesPaths() {
        List<Path> ret = new ArrayList<Path>();
        if (measureType != null)
            ret.add(new Path(measureType));
        return ret;
    }
}
