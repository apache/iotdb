package cn.edu.tsinghua.tsfile.read.expression;

import cn.edu.tsinghua.tsfile.read.common.Path;

import java.util.ArrayList;
import java.util.List;


public class QueryExpression {
    private List<Path> selectedSeries;
    private IExpression expression;
    private boolean hasQueryFilter;

    private QueryExpression() {
        selectedSeries = new ArrayList<>();
        hasQueryFilter = false;
    }

    public static QueryExpression create() {
        return new QueryExpression();
    }

    public static QueryExpression create(List<Path> selectedSeries, IExpression expression) {
        QueryExpression ret = new QueryExpression();
        ret.selectedSeries = selectedSeries;
        ret.expression = expression;
        ret.hasQueryFilter = expression != null;
        return ret;
    }

    public QueryExpression addSelectedPath(Path path) {
        this.selectedSeries.add(path);
        return this;
    }

    public QueryExpression setExpression(IExpression expression) {
        if (expression != null) {
            this.expression = expression;
            hasQueryFilter = true;
        }
        return this;
    }

    public QueryExpression setSelectSeries(List<Path> selectedSeries) {
        this.selectedSeries = selectedSeries;
        return this;
    }

    public IExpression getExpression() {
        return expression;
    }

    public List<Path> getSelectedSeries() {
        return selectedSeries;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("\n\t[Selected Series]:").append(selectedSeries)
                .append("\n\t[expression]:").append(expression);
        return stringBuilder.toString();
    }

    public boolean hasQueryFilter() {
        return hasQueryFilter;
    }
}
