package cn.edu.thu.tsfiledb.qp.logical.crud;

import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.KW_AND;
import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.KW_OR;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfiledb.qp.exception.LogicalOperatorException;
import cn.edu.thu.tsfiledb.qp.logical.Operator;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;

/**
 * This class is for filter operator and implements
 * {@link Operator} .<br>
 * it may consist of more than two child FilterOperator, but if it's not leaf operator, the relation
 * is same among all of its children.(AND or OR). It's identified by tokenType.
 *
 * @author kangrong
 * @author qiaojialin
 *
 */
public class FilterOperator extends Operator implements Comparable<FilterOperator> {
    // it is the symbol of token. e.g. AND is & and OR is |
    protected String tokenSymbol;

    protected List<FilterOperator> childOperators;
    // leaf filter operator means it doesn't have left and right child filterOperator. Leaf filter
    // should set FunctionOperator.
    protected boolean isLeaf = false;
    // isSingle being true means all recursive children of this filter belong to one series path.
    protected boolean isSingle = false;
    // if isSingle = false, singlePath must be null
    protected Path singlePath = null;

    public FilterOperator(int tokenType) {
        super(tokenType);
        operatorType = OperatorType.FILTER;
        childOperators = new ArrayList<>();
        this.tokenIntType = tokenType;
        isLeaf = false;
        tokenSymbol = SQLConstant.tokenSymbol.get(tokenType);
    }

    public void setTokenIntType(int intType)  {
        this.tokenIntType = intType;
        this.tokenName = SQLConstant.tokenNames.get(tokenIntType);
        this.tokenSymbol = SQLConstant.tokenSymbol.get(tokenIntType);
    }

    public FilterOperator(int tokenType, boolean isSingle) {
        this(tokenType);
        this.isSingle = isSingle;
    }

    public int getTokenIntType() {
        return tokenIntType;
    }

    public List<FilterOperator> getChildren() {
        return childOperators;
    }

    public void setChildren(List<FilterOperator> children) {
        this.childOperators = children;
    }

    public void setIsSingle(boolean b) {
        this.isSingle = b;
    }

    /**
     * if this node's singlePath is set, this.isLeaf will be set true in same time
     *
     * @param path operator path
     */
    public void setSinglePath(Path path) {
        this.singlePath = path;
        this.isLeaf = true;
    }

    public Path getSinglePath() {
        return singlePath;
    }

    public boolean addChildOperator(FilterOperator op) {
        childOperators.add(op);
        return true;
    }

    /**
     * For a filter operator, if isSingle, call transformToSingleFilter.<br>
     * FilterOperator cannot be leaf.
     *
     * @return filter in TsFile
     * @throws QueryProcessorException
     */
    public FilterExpression transformToFilterExpression(QueryProcessExecutor executor, FilterSeriesType type)
            throws QueryProcessorException {
        if (isSingle) {
            Pair<SingleSeriesFilterExpression, String> ret = transformToSingleFilter(executor, type);
            return ret.left;
        } else {
            if (childOperators.isEmpty()) {
                throw new LogicalOperatorException("this filter is not leaf, but it's empty:"
                        + tokenIntType);
            }
            FilterExpression retFilter = childOperators.get(0).transformToFilterExpression(executor, type);
            FilterExpression currentFilter;
            for (int i = 1; i < childOperators.size(); i++) {
                currentFilter = childOperators.get(i).transformToFilterExpression(executor, type);
                switch (tokenIntType) {
                    case KW_AND:
                        retFilter = FilterFactory.and(retFilter, currentFilter);
                        break;
                    case KW_OR:
                        retFilter = FilterFactory.or(retFilter, currentFilter);
                        break;
                    default:
                        throw new LogicalOperatorException("unknown binary tokenIntType:"
                                + tokenIntType + ",maybe it means "
                                + SQLConstant.tokenNames.get(tokenIntType));
                }
            }
            return retFilter;
        }
    }

    /**
     * it will be used in BasicFunction Operator
     *
     * @param executor physical plan executor
     * @param type series type
     * @return - pair.left:SingleSeriesFilterExpression constructed by its one child; - pair.right: Path
     *         represented by this child.
     * @throws QueryProcessorException exception in filter transforming
     */
    protected Pair<SingleSeriesFilterExpression, String> transformToSingleFilter(QueryProcessExecutor executor, FilterSeriesType type)
            throws QueryProcessorException {
        if (childOperators.isEmpty()) {
            throw new LogicalOperatorException(
                    ("transformToSingleFilter: this filter is not leaf, but it's empty:{}" + tokenIntType));
        }
        Pair<SingleSeriesFilterExpression, String> currentPair =
                childOperators.get(0).transformToSingleFilter(executor, type);

        SingleSeriesFilterExpression retFilter = currentPair.left;
        String path = currentPair.right;
        //
        for (int i = 1; i < childOperators.size(); i++) {
            currentPair = childOperators.get(i).transformToSingleFilter(executor, type);
            if (!path.equals(currentPair.right))
                throw new LogicalOperatorException(
                        ("transformToSingleFilter: paths among children are not inconsistent: one is:"
                                + path + ",another is:" + currentPair.right));
            switch (tokenIntType) {
                case KW_AND:
                    retFilter = (SingleSeriesFilterExpression) FilterFactory.and(retFilter, currentPair.left);
                    break;
                case KW_OR:
                    retFilter = (SingleSeriesFilterExpression) FilterFactory.or(retFilter, currentPair.left);
                    break;
                default:
                    throw new LogicalOperatorException("unknown binary tokenIntType:"
                            + tokenIntType + ",maybe it means "
                            + SQLConstant.tokenNames.get(tokenIntType));
            }
        }
        return new Pair<>(retFilter, path);
    }

    /**
     * if this is null, ordered to later
     */
    @Override
    public int compareTo(FilterOperator fil) {
        if (singlePath == null && fil.singlePath == null) {
            return 0;
        }
        if (singlePath == null) {
            return 1;
        }
        if (fil.singlePath == null) {
            return -1;
        }
        return fil.singlePath.toString().compareTo(singlePath.toString());
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public boolean isSingle() {
        return isSingle;
    }

    public String showTree() {
        return showTree(0);
    }

    public String showTree(int spaceNum) {
        StringContainer sc = new StringContainer();
        for (int i = 0; i < spaceNum; i++) {
            sc.addTail("  ");
        }
        sc.addTail(this.tokenName);
        if (isSingle) {
            sc.addTail("[single:", getSinglePath().toString(), "]");
        }
        sc.addTail("\n");
        for (FilterOperator filter : childOperators) {
            sc.addTail(filter.showTree(spaceNum + 1));
        }
        return sc.toString();
    }

    @Override
    public String toString() {
        StringContainer sc = new StringContainer();
        sc.addTail("[", this.tokenName);
        if (isSingle) {
            sc.addTail("[single:", getSinglePath().toString(), "]");
        }
        sc.addTail(" ");
        for (FilterOperator filter : childOperators) {
            sc.addTail(filter.toString());
        }
        sc.addTail("]");
        return sc.toString();
    }

    @Override
    public FilterOperator clone() {
        FilterOperator ret = new FilterOperator(this.tokenIntType);
        ret.tokenSymbol=tokenSymbol;
        ret.isLeaf = isLeaf;
        ret.isSingle = isSingle;
        if(singlePath != null)
            ret.singlePath = singlePath.clone();
        for (FilterOperator filterOperator : this.childOperators) {
            ret.addChildOperator(filterOperator.clone());
        }
        return ret;
    }
}
