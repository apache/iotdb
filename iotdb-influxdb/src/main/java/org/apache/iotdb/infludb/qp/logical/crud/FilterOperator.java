package org.apache.iotdb.infludb.qp.logical.crud;

import org.apache.iotdb.infludb.qp.constant.FilterConstant;

import java.util.ArrayList;
import java.util.List;

public class FilterOperator implements Comparable<FilterOperator> {

    public FilterConstant.FilterType getFilterType() {
        return filterType;
    }

    public void setFilterType(FilterConstant.FilterType filterType) {
        this.filterType = filterType;
    }

    public List<FilterOperator> getChildOperators() {
        return childOperators;
    }

    public void setChildOperators(List<FilterOperator> childOperators) {
        this.childOperators = childOperators;
    }

    public String getKeyName() {
        return keyName;
    }

    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }

    protected FilterConstant.FilterType filterType;

    private List<FilterOperator> childOperators = new ArrayList<>();

    String keyName;

    public FilterOperator() {
    }

    public FilterOperator(FilterConstant.FilterType filterType) {
        this.filterType = filterType;
    }

    public List<FilterOperator> getChildren() {
        return childOperators;
    }

    public boolean addChildOperator(FilterOperator op) {
        childOperators.add(op);
        return true;
    }

    @Override
    public int compareTo(FilterOperator o) {
        return 0;
    }
}
