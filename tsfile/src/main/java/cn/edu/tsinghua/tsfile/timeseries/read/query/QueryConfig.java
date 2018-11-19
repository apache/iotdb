package cn.edu.tsinghua.tsfile.timeseries.read.query;

import java.util.ArrayList;

public class QueryConfig {

    private String timeFilter;
    private String freqFilter;
    private ArrayList<String> selectColumns;
    private String valueFilter;
    private QueryType queryType;

    /**
     * Construct a queryConfig for QUERY_WITHOUT_FILTER
     *
     * @param selects selected columns
     */
    public QueryConfig(String selects) {
        this.selectColumns = new ArrayList<>();
        String[] cols = selects.split("\\|");
        for (String col : cols) {
            selectColumns.add(col);
        }
        this.queryType = QueryType.QUERY_WITHOUT_FILTER;
    }

    /**
     * Construct a queryConfig automatically according to the filters
     *
     * @param selects selected columns
     * @param timeFilter time filter
     * @param freqFilter frequency filter
     * @param valueFilter value filter
     */
    public QueryConfig(String selects, String timeFilter, String freqFilter,
                       String valueFilter) {
        this.selectColumns = new ArrayList<String>();
        String[] cols = selects.split("\\|");

        for (String col : cols) {
            selectColumns.add(col);
        }

        this.setTimeFilter(timeFilter);
        this.setFreqFilter(freqFilter);
        this.setValueFilter(valueFilter);

        if (timeFilter.equals("null") && freqFilter.equals("null") && valueFilter.equals("null")) {
            this.queryType = QueryType.QUERY_WITHOUT_FILTER;
        } else if (valueFilter.startsWith("[")) {
            this.queryType = QueryType.CROSS_QUERY;
        } else {
            this.queryType = QueryType.SELECT_ONE_COL_WITH_FILTER;
        }
    }

    public QueryConfig(ArrayList<String> selectColumns, String timeFilter, String freqFilter,
                       String valueFilter) {
        this.selectColumns = selectColumns;
        this.setTimeFilter(timeFilter);
        this.setFreqFilter(freqFilter);
        this.setValueFilter(valueFilter);
    }


    public ArrayList<String> getSelectColumns() {
        return selectColumns;
    }

    public void setSelectColumns(ArrayList<String> selectColumns) {
        this.selectColumns = selectColumns;
    }

    public String getTimeFilter() {
        return timeFilter;
    }

    public void setTimeFilter(String timeFilter) {
        this.timeFilter = timeFilter;
    }

    public String getFreqFilter() {
        return freqFilter;
    }

    private void setFreqFilter(String freqFilter) {
        this.freqFilter = freqFilter;
    }

    public String getValueFilter() {
        return valueFilter;
    }

    private void setValueFilter(String valueFilter) {
        this.valueFilter = valueFilter;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public void setQueryType(QueryType queryType) {
        this.queryType = queryType;
    }


}
