package cn.edu.thu.tsfiledb.qp.logical.sys;

import cn.edu.thu.tsfiledb.qp.logical.RootOperator;

/**
 * this class maintains information in Author statement, including CREATE, DROP, GRANT and REVOKE
 * 
 * @author kangrong
 *
 */
public class LoadDataOperator extends RootOperator {
    private final String inputFilePath;
    private final String measureType;

    public LoadDataOperator(int tokenIntType, String inputFilePath, String measureType) {
        super(tokenIntType);
        operatorType = OperatorType.LOADDATA;
        this.inputFilePath = inputFilePath;
        this.measureType = measureType;
    }

    public String getInputFilePath() {
        return inputFilePath;
    }

    public String getMeasureType() {
        return measureType;
    }
}
