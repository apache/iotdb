package cn.edu.thu.tsfiledb.qp.logical.operator.crud;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.qp.exception.logical.operator.QpSelectFromException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;
import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.RootOperator;

/**
 * SFWOperator includes four subclass: INSERT,DELETE,UPDATE,QUERY. All of these four statements has
 * three partition: select clause, from clause and filter clause(where clause).
 * 
 * @author kangrong
 *
 */
public abstract class SFWOperator extends RootOperator {
    private static final Logger LOG = LoggerFactory.getLogger(SFWOperator.class);

    protected SelectOperator selectOperator;
    protected FromOperator fromOperator;
    protected FilterOperator filterOperator;

    public SFWOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.ROOT;
    }

    // /**
    // * transform this root operator tree to a physical plan tree.Node that, before this method
    // * called, the where filter has been dealt with
    // * {@linkplain com.corp.tsfile.qp.logical.optimizer.filter.MergeSingleFilterOptimizer}
    // *
    // */
    // public abstract PhysicalPlan transformToPhysicalPlan(QueryProcessExecutor conf)
    // throws QueryProcessorException;

    public void setSelectOperator(SelectOperator sel) {
        this.selectOperator = sel;
    }

    public void setFromOperator(FromOperator from) {
        this.fromOperator = from;
    }

    public FromOperator getFrom() {
        return fromOperator;
    }

    public SelectOperator getSelect() {
        return selectOperator;
    }

    public void setFilterOperator(FilterOperator filter) {
        this.filterOperator = filter;
    }

    public FilterOperator getFilter() {
        return filterOperator;
    }

    /**
     * get information from SelectOperator and FromOperator and generate all table paths. <b>Note
     * that</b>, if there are some path doesn't exist in metadata tree or file metadata, this method
     * just log error records and <b>omit</b> them. Nevertheless, if all of paths doesn't exist, it
     * will throw <b>Exception</b>.
     * 
     * @return - a list of path
     * @throws QpSelectFromException
     */
    public List<Path> getSelSeriesPaths(QueryProcessExecutor qpConfig) throws QpSelectFromException {
        List<Path> prefixPaths = null;
        if (fromOperator != null) {
            prefixPaths = fromOperator.getPrefixPaths();
            // check legality of from clauses
            if (!qpConfig.isSingleFile()) {
                for (Path path : prefixPaths) {
                    if (!path.startWith(SQLConstant.ROOT))
                        throw new QpSelectFromException(
                                "given select clause path doesn't start with ROOT!" + path);
                }
            }
        }
        // after ConcatPathOptimizer, paths in FROM clause are just used to check legality for delta
        // system
        List<Path> suffixPaths = null;
        if (selectOperator != null)
            suffixPaths = selectOperator.getSuffixPaths();
        if ((suffixPaths == null || suffixPaths.isEmpty())) {
            // Log.error("from clause and select clause cannot be both empty!");
            throw new QpSelectFromException("select clause cannot be empty!");
        }
        else
            return suffixPaths;
    }
}
