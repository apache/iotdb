package cn.edu.thu.tsfiledb.qp.logical.operator.crud;

import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.KW_AND;
import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.KW_OR;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.exception.logical.operator.FilterOperatorException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;
import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.Operator;

/**
 * This class is for filter operator and implements
 * {@link cn.edu.thu.tsfiledb.qp.logical.operator.Operator} .<br>
 * it may consist of more than two child FilterOperator, but if it's not leaf operator, the relation
 * is same among all of its children.(AND or OR). It's identified by tokenType.
 * 
 * @author kangrong
 *
 */
public class FilterOperator extends Operator implements Comparable<FilterOperator> {
    Logger LOG = LoggerFactory.getLogger(FilterOperator.class);
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

    public void addHeadDeltaObjectPath(String deltaObject) {
        for (FilterOperator child : childOperators) {
            child.addHeadDeltaObjectPath(deltaObject);
        }
        if(isSingle)
            this.singlePath.addHeadPath(deltaObject);
    }

    public FilterOperator(int tokenType) {
        super(tokenType);
        operatorType = OperatorType.FILTER;
        childOperators = new ArrayList<FilterOperator>();
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

    public void setChildrenList(List<FilterOperator> children) {
        this.childOperators = children;
    }

    public void setIsSingle(boolean b) {
        this.isSingle = b;
    }

    /**
     * if this node's singlePath is set, this.isLeaf will be set true in same time
     * 
     * @param p
     */
    public void setSinglePath(Path p) {
        this.singlePath = p;
    }

    public Path getSinglePath() {
        return singlePath;
    }

    public boolean addChildOPerator(FilterOperator op) {
        childOperators.add((FilterOperator) op);
        return true;
    }


    /**
     * For a filter operator, if isSingle, call transformToSingleFilter.<br>
     * FilterOperator cannot be leaf.
     * 
     * @return
     * @throws QueryProcessorException
     */
    public FilterExpression transformToFilter(QueryProcessExecutor conf)
            throws QueryProcessorException {
        if (isSingle) {
            Pair<SingleSeriesFilterExpression, String> ret = transformToSingleFilter(conf);
            return ret.left;
        } else {
            if (childOperators.isEmpty()) {
                throw new FilterOperatorException("this filter is not leaf, but it's empty:"
                        + tokenIntType);
            }
            FilterExpression retFilter = childOperators.get(0).transformToFilter(conf);
            FilterExpression cross;
            for (int i = 1; i < childOperators.size(); i++) {
                cross = childOperators.get(i).transformToFilter(conf);
                switch (tokenIntType) {
                    case KW_AND:
                        retFilter = FilterFactory.and(retFilter, cross);
                        break;
                    case KW_OR:
                        retFilter = FilterFactory.or(retFilter, cross);
                        break;
                    default:
                        throw new FilterOperatorException("unknown binary tokenIntType:"
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
     * @param exec
     * @return - pair.left:SingleSeriesFilterExpression constructed by its one child; - pair.right: Path
     *         represented by this child.
     * @throws QueryProcessorException
     */
    protected Pair<SingleSeriesFilterExpression, String> transformToSingleFilter(QueryProcessExecutor exec)
            throws QueryProcessorException {
        if (childOperators.isEmpty()) {
            throw new FilterOperatorException(
                    ("transformToSingleFilter: this filter is not leaf, but it's empty:{}" + tokenIntType));
        }
        Pair<SingleSeriesFilterExpression, String> single =
                childOperators.get(0).transformToSingleFilter(exec);
        SingleSeriesFilterExpression retFilter = single.left;
        String childSeriesStr = single.right;
        //
        for (int i = 1; i < childOperators.size(); i++) {
            single = childOperators.get(i).transformToSingleFilter(exec);
            if (!childSeriesStr.equals(single.right))
                throw new FilterOperatorException(
                        ("transformToSingleFilter: paths among children are not inconsistent: one is:"
                                + childSeriesStr + ",another is:" + single.right));
            switch (tokenIntType) {
                case KW_AND:
                    retFilter = (SingleSeriesFilterExpression) FilterFactory.and(retFilter, single.left);
                    break;
                case KW_OR:
                    retFilter = (SingleSeriesFilterExpression) FilterFactory.or(retFilter, single.left);
                    break;
                default:
                    throw new FilterOperatorException("unknown binary tokenIntType:"
                            + tokenIntType + ",maybe it means "
                            + SQLConstant.tokenNames.get(tokenIntType));
            }
        }
        return new Pair<SingleSeriesFilterExpression, String>(retFilter, childSeriesStr);
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
            ret.addChildOPerator(filterOperator.clone());
        }
        return ret;
    }
}
