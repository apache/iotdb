package org.apache.iotdb.db.qp.logical.crud;

import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.List;

/**
 * SFWOperator(select-from-where) includes four subclass: INSERT,DELETE,UPDATE,QUERY. All of these four statements has
 * three partition: select clause, from clause and filter clause(where clause).
 */
public abstract class SFWOperator extends RootOperator {

    private SelectOperator selectOperator;
    private FromOperator fromOperator;
    private FilterOperator filterOperator;
    private boolean hasAggregation = false;

    public SFWOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.SFW;
    }

    public void setSelectOperator(SelectOperator sel) {
        this.selectOperator = sel;
        if(!sel.getAggregations().isEmpty()) {
            hasAggregation = true;
        }
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
     * @return - a list of seriesPath
     */
    public List<Path> getSelectedPaths() {
        List<Path> suffixPaths = null;
        if (selectOperator != null) {
            suffixPaths = selectOperator.getSuffixPaths();
        }
        return suffixPaths;
    }

    public boolean hasAggregation() {
        return hasAggregation;
    }
}
