package org.apache.iotdb.infludb.query.expression.unary;

import org.apache.iotdb.infludb.query.expression.Expression;

import java.util.*;

public class FunctionExpression implements Expression {

    private final String functionName;
    private final Map<String, String> functionAttributes;

    private List<Expression> expressions;

    private String expressionString;
    private String parametersString;

    private final boolean isAggregationFunctionExpression;

    public FunctionExpression(String functionName) {
        this.functionName = functionName;
        functionAttributes = new LinkedHashMap<>();
        expressions = new ArrayList<>();
        isAggregationFunctionExpression = true;
    }


    public void addAttribute(String key, String value) {
        functionAttributes.put(key, value);
    }

    public void addExpression(Expression expression) {
        expressions.add(expression);
    }

    public void setExpressions(List<Expression> expressions) {
        this.expressions = expressions;
    }

    public String getFunctionName() {
        return functionName;
    }

    public Map<String, String> getFunctionAttributes() {
        return functionAttributes;
    }

    public List<Expression> getExpressions() {
        return expressions;
    }


    @Override
    public String toString() {
        if (expressionString == null) {
            expressionString = functionName + "(" + getParametersString() + ")";
        }
        return expressionString;
    }

    /**
     * Generates the parameter part of the function column name.
     *
     * <p>Example:
     *
     * <p>Full column name -> udf(root.sg.d.s1, sin(root.sg.d.s1), 'key1'='value1', 'key2'='value2')
     *
     * <p>The parameter part -> root.sg.d.s1, sin(root.sg.d.s1), 'key1'='value1', 'key2'='value2'
     */
    public String getParametersString() {
        if (parametersString == null) {
            StringBuilder builder = new StringBuilder();
            if (!expressions.isEmpty()) {
                builder.append(expressions.get(0).toString());
                for (int i = 1; i < expressions.size(); ++i) {
                    builder.append(", ").append(expressions.get(i).toString());
                }
            }
            if (!functionAttributes.isEmpty()) {
                if (!expressions.isEmpty()) {
                    builder.append(", ");
                }
                Iterator<Map.Entry<String, String>> iterator = functionAttributes.entrySet().iterator();
                Map.Entry<String, String> entry = iterator.next();
                builder
                        .append("\"")
                        .append(entry.getKey())
                        .append("\"=\"")
                        .append(entry.getValue())
                        .append("\"");
                while (iterator.hasNext()) {
                    entry = iterator.next();
                    builder
                            .append(", ")
                            .append("\"")
                            .append(entry.getKey())
                            .append("\"=\"")
                            .append(entry.getValue())
                            .append("\"");
                }
            }
            parametersString = builder.toString();
        }
        return parametersString;
    }
}
