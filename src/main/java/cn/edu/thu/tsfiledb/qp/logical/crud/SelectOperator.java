package cn.edu.thu.tsfiledb.qp.logical.crud;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.logical.Operator;

/**
 * this class maintains information from select clause
 * 
 * @author kangrong
 *
 */
public final class SelectOperator extends Operator {

    private List<Path> suffixList;
    private String aggregation;

    public SelectOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.SELECT;
        suffixList = new ArrayList<>();
    }

    public void addSuffixTablePath(Path suffixPath) {
        suffixList.add(suffixPath);
    }

    public void addSuffixTablePath(Path suffixPath, String aggregation) {
        suffixList.add(suffixPath);
        this.aggregation = aggregation;
    }

    public String getAggregation(){
        return this.aggregation;
    }

    public void setSuffixPathList(List<Path> suffixPaths) {
        suffixList = suffixPaths;
    }

    public List<Path> getSuffixPaths() {
        return suffixList;
    }

}
