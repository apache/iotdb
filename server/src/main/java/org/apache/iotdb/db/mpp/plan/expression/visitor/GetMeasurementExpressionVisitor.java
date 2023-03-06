package org.apache.iotdb.db.mpp.plan.expression.visitor;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;

import java.util.ArrayList;
import java.util.List;

public class GetMeasurementExpressionVisitor extends ReconstructVisitor {
    @Override
    public Expression visitFunctionExpression(FunctionExpression functionExpression, Void context) {
        List<Expression> childExpressions = new ArrayList<>();
        for (Expression suffixExpression : functionExpression.getExpressions()) {
            childExpressions.add(process(suffixExpression, null));
        }
        return new FunctionExpression(
                ((FunctionExpression) functionExpression).getFunctionName(),
                ((FunctionExpression) functionExpression).getFunctionAttributes(),
                childExpressions);
    }

    @Override
    public Expression visitTimeSeriesOperand(TimeSeriesOperand timeSeriesOperand, Void context) {
        MeasurementPath rawPath = (MeasurementPath) timeSeriesOperand.getPath();
        PartialPath measurement = new PartialPath(rawPath.getMeasurement(), false);
        MeasurementPath measurementWithSchema =
                new MeasurementPath(measurement, rawPath.getMeasurementSchema());
        if (rawPath.isMeasurementAliasExists()) {
            measurementWithSchema.setMeasurementAlias(rawPath.getMeasurementAlias());
        }
        measurementWithSchema.setTagMap(rawPath.getTagMap());
        return new TimeSeriesOperand(measurementWithSchema);
    }
}
