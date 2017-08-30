package cn.edu.thu.tsfiledb.qp.logical.crud;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfiledb.qp.logical.Operator;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;

/**
 * this class maintains information from select clause
 *
 * @author kangrong
 * @author qiaojialin
 *
 */
public final class SelectOperator extends Operator {

    private List<Path> suffixList;
    private List<String> aggregations;

    public SelectOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.SELECT;
        suffixList = new ArrayList<>();
        aggregations = new ArrayList<>();
    }

    public void addSelectPath(Path suffixPath) {
        suffixList.add(suffixPath);
    }

    public void addClusterPath(Path suffixPath, String aggregation) {
        suffixList.add(suffixPath);
        aggregations.add(aggregation);
    }

    public List<String> getAggregations(){
        return this.aggregations;
    }

    public void setSuffixPathList(List<Path> suffixPaths) {
        suffixList = suffixPaths;
    }

    public List<Path> getSuffixPaths() {
        return suffixList;
    }

}
