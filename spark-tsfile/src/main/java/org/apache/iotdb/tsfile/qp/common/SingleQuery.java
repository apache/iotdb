package org.apache.iotdb.tsfile.qp.common;


import java.util.List;

/**
 * This class is constructed with a single getIndex plan. Single getIndex means it could be processed by
 * reading API by one pass directly.<br>
 *
 */
public class SingleQuery {

    private List<FilterOperator> columnFilterOperators;
    private FilterOperator timeFilterOperator;
    private FilterOperator valueFilterOperator;

    public SingleQuery(List<FilterOperator> columnFilterOperators,
                       FilterOperator timeFilter, FilterOperator valueFilter) {
        super();
        this.columnFilterOperators = columnFilterOperators;
        this.timeFilterOperator = timeFilter;
        this.valueFilterOperator = valueFilter;
    }

    public List<FilterOperator> getColumnFilterOperator() {

        return columnFilterOperators;
    }

    public FilterOperator getTimeFilterOperator() {
        return timeFilterOperator;
    }

    public FilterOperator getValueFilterOperator() {
        return valueFilterOperator;
    }

    @Override
    public String toString() {
        return "SingleQuery: \n" + columnFilterOperators + "\n" + timeFilterOperator + "\n" + valueFilterOperator;
    }


}
