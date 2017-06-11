package cn.edu.thu.tsfiledb.qp.logical.operator.root.sfw;

import java.util.List;

import cn.edu.thu.tsfiledb.qp.logical.operator.clause.SelectOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.clause.filter.FilterOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.clause.FromOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.qp.exception.logical.operator.QpSelectFromException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.root.RootOperator;

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
        operatorType = OperatorType.SFW;
    }

    public void setSelectOperator(SelectOperator sel) {
        this.selectOperator = sel;
    }

    public void setFromOperator(FromOperator from) {
        this.fromOperator = from;
    }

    public void setFilterOperator(FilterOperator filter) {
        this.filterOperator = filter;
    }

    public FromOperator getFromOperator() {
        return fromOperator;
    }

    public SelectOperator getSelectOperator() {
        return selectOperator;
    }

    public FilterOperator getFilterOperator() {
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
    public List<Path> getSelSeriesPaths(QueryProcessExecutor executor) throws QpSelectFromException {
        List<Path> prefixPaths;
        if (fromOperator != null) {
            prefixPaths = fromOperator.getPrefixPaths();
            // check legality of from clauses
            if (!executor.isSingleFile()) {
                for (Path path : prefixPaths) {
                    if (!path.startWith(SQLConstant.ROOT))
                        throw new QpSelectFromException(
                                "given select clause path doesn't start with SFW!" + path);
                }
            }
        }
        // after ConcatPathOptimizer, paths in FROM clause are just used to check legality for delta
        // system
        List<Path> suffixPaths = null;
        if (selectOperator != null)
            suffixPaths = selectOperator.getSuffixPaths();
        if ((suffixPaths == null || suffixPaths.isEmpty())) {
            throw new QpSelectFromException("select clause cannot be empty!");
        }
        else
            return suffixPaths;
    }
}
