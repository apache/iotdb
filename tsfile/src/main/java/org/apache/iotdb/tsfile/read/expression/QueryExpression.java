/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.read.expression;

import org.apache.iotdb.tsfile.read.common.Path;

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
