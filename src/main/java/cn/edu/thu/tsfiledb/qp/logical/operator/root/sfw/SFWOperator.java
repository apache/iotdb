package cn.edu.thu.tsfiledb.qp.logical.operator.root.sfw;

import java.util.List;

import cn.edu.thu.tsfiledb.qp.logical.operator.clause.SelectOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.clause.filter.FilterOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.clause.FromOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.qp.exception.logical.operator.QpSelectFromException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
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
     * get information from SelectOperator and FromOperator and generate all table paths.
     * 
     * @return - a list of path
     * @throws QpSelectFromException
     */
    public List<Path> getSelectedPaths() throws QpSelectFromException {
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
