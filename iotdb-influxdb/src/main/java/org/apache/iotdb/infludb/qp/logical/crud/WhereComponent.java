package org.apache.iotdb.infludb.qp.logical.crud;

public class WhereComponent {

    private FilterOperator filterOperator;

    public WhereComponent() {
    }

    public WhereComponent(FilterOperator filterOperator) {
        this.filterOperator = filterOperator;
    }

    public FilterOperator getFilterOperator() {
        return filterOperator;
    }

    public void setFilterOperator(FilterOperator filterOperator) {
        this.filterOperator = filterOperator;
    }
}
