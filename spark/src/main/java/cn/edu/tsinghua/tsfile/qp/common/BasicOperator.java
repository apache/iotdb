package cn.edu.tsinghua.tsfile.qp.common;


import cn.edu.tsinghua.tsfile.qp.exception.BasicOperatorException;

/**
 * basic operators include < > >= <= !=.
 *
 */

public class BasicOperator extends FilterOperator {

    private String seriesPath;
    private String seriesValue;

    public String getSeriesPath() {
        return seriesPath;
    }

    public String getSeriesValue() {
        return seriesValue;
    }

    public BasicOperator(int tokenIntType, String path, String value) {
        super(tokenIntType);
        this.seriesPath = this.singlePath = path;
        this.seriesValue = value;
        this.isLeaf = true;
        this.isSingle = true;
    }

    public void setReversedTokenIntType() throws BasicOperatorException {
        int intType = SQLConstant.reverseWords.get(tokenIntType);
        setTokenIntType(intType);
    }

    @Override
    public String getSinglePath() {
        return singlePath;
    }


    @Override
    public BasicOperator clone() {
        BasicOperator ret;
        ret = new BasicOperator(this.tokenIntType, seriesPath, seriesValue);
        ret.tokenSymbol=tokenSymbol;
        ret.isLeaf = isLeaf;
        ret.isSingle = isSingle;
        return ret;
    }
    
    @Override
    public String toString() {
        return "[" + seriesPath + tokenSymbol + seriesValue + "]";
    }
}
