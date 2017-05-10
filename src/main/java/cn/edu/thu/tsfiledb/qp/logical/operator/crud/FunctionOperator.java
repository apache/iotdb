package cn.edu.thu.tsfiledb.qp.logical.operator.crud;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;

/**
 * This class presents series condition which is general(e.g. numerical comparison) or defined by
 * user. Function is used for bottom operator.<br>
 * FunctionOperator has a {@code seriesPath}, and other filter condition.
 * 
 * @author kangrong
 *
 */

public class FunctionOperator extends FilterOperator {
    Logger LOG = LoggerFactory.getLogger(FunctionOperator.class);

    public FunctionOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.FUNC;
        isLeaf = true;
        isSingle = true;
    }

    @Override
    public boolean addChildOPerator(FilterOperator op) {
        LOG.error("cannot add child to leaf FilterOperator, now it's FunctionOperator");
        return false;
    }


    /**
     * For function Operator, it just return itself expression and its path.
     * 
     * @param reserveWordsMap
     * @return {@code <reservedWord, expression>}
     */
    public Pair<String, StringContainer> getFilterToStr() {
        throw new UnsupportedOperationException();
    }

    /**
     * return seriesPath
     * 
     * @return
     */
    public String getSeriesPath() {
        throw new UnsupportedOperationException();
    }
    
}
