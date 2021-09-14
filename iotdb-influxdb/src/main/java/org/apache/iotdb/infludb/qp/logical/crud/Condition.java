package org.apache.iotdb.infludb.qp.logical.crud;

import org.apache.iotdb.infludb.qp.constant.FilterConstant;

public class Condition {

    //当前查询过滤条件的var
    public String Value;
    public FilterConstant.FilterType FilterType;
    //当前查询过滤条件的实际值
    public String Literal;

    public Condition(String value, FilterConstant.FilterType filterType, String literal) {
        Value = value;
        FilterType = filterType;
        Literal = literal;
    }

    public Condition() {
    }

    public void setValue(String value) {
        Value = value;
    }

    public void setFilterType(FilterConstant.FilterType filterType) {
        FilterType = filterType;
    }

    public void setLiteral(String literal) {
        Literal = literal;
    }

    public String getValue() {
        return Value;
    }

    public FilterConstant.FilterType getFilterType() {
        return FilterType;
    }

    public String getLiteral() {
        return Literal;
    }
}
