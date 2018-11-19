package cn.edu.tsinghua.iotdb.qp.strategy.optimizer;

import static cn.edu.tsinghua.iotdb.qp.constant.SQLConstant.KW_AND;
import static cn.edu.tsinghua.iotdb.qp.constant.SQLConstant.KW_NOT;
import static cn.edu.tsinghua.iotdb.qp.constant.SQLConstant.KW_OR;

import java.util.List;

import cn.edu.tsinghua.iotdb.qp.constant.SQLConstant;
import cn.edu.tsinghua.iotdb.qp.exception.LogicalOptimizeException;
import cn.edu.tsinghua.iotdb.qp.exception.LogicalOperatorException;
import cn.edu.tsinghua.iotdb.qp.logical.crud.BasicFunctionOperator;
import cn.edu.tsinghua.iotdb.qp.logical.crud.FilterOperator;

public class RemoveNotOptimizer implements IFilterOptimizer {

    /**
     * get DNF(disjunctive normal form) for this filter operator tree. Before getDNF, this op tree
     * must be binary, in another word, each non-leaf node has exactly two children.
     * 
     * @return optimized operator
     * @throws LogicalOptimizeException exception in RemoveNot optimizing
     */
    @Override
    public FilterOperator optimize(FilterOperator filter) throws LogicalOperatorException {
        return removeNot(filter);
    }

    private FilterOperator removeNot(FilterOperator filter) throws LogicalOperatorException {
        if (filter.isLeaf())
            return filter;
        int tokenInt = filter.getTokenIntType();
        switch (tokenInt) {
            case KW_AND:
            case KW_OR:
                // replace children in-place for efficiency
                List<FilterOperator> children = filter.getChildren();
                children.set(0, removeNot(children.get(0)));
                children.set(1, removeNot(children.get(1)));
                return filter;
            case KW_NOT:
                return reverseFilter(filter.getChildren().get(0));
            default:
                throw new LogicalOptimizeException("Unknown token in removeNot: " + tokenInt + ","
                        + SQLConstant.tokenNames.get(tokenInt));
        }
    }

    /**
     * reverse given filter to reversed expression
     * 
     * @param filter BasicFunctionOperator
     * @return FilterOperator reversed BasicFunctionOperator
     * @throws LogicalOptimizeException exception in reverse filter
     */
    private FilterOperator reverseFilter(FilterOperator filter) throws LogicalOperatorException {
        int tokenInt = filter.getTokenIntType();
        if (filter.isLeaf()) {
            try {
                ((BasicFunctionOperator) filter).setReversedTokenIntType();
            } catch (LogicalOperatorException e) {
                throw new LogicalOperatorException(
                        "convert BasicFuntion to reserved meet failed: previous token:" + tokenInt
                                + "tokenName:" + SQLConstant.tokenNames.get(tokenInt));
            }
            return filter;
        }
        switch (tokenInt) {
            case KW_AND:
            case KW_OR:
                List<FilterOperator> children = filter.getChildren();
                children.set(0, reverseFilter(children.get(0)));
                children.set(1, reverseFilter(children.get(1)));
                filter.setTokenIntType(SQLConstant.reverseWords.get(tokenInt));
                return filter;
            case KW_NOT:
                return removeNot(filter.getChildren().get(0));
            default:
                throw new LogicalOptimizeException("Unknown token in reverseFilter: " + tokenInt + ","
                        + SQLConstant.tokenNames.get(tokenInt));
        }
    }

}
