package cn.edu.thu.tsfiledb.qp.exception.logical.operator;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.logical.common.filter.FilterOperator;

/**
 * This exception is threw whiling meeting error in
 * {@linkplain FilterOperator FilterOperator}
 * 
 * @author kangrong
 *
 */
public class FilterOperatorException extends QueryProcessorException {

    private static final long serialVersionUID = 167597682291449523L;

    public FilterOperatorException(String msg) {
        super(msg);
    }

}
