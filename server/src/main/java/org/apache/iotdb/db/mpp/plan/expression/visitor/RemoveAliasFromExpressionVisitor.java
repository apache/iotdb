package org.apache.iotdb.db.mpp.plan.expression.visitor;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;

import java.util.ArrayList;
import java.util.List;

public class RemoveAliasFromExpressionVisitor extends ReconstructVisitor {
    @Override
    public Expression visitFunctionExpression(FunctionExpression functionExpression, Void context) {
        List<Expression> childResult = new ArrayList<>();
        functionExpression.getExpressions().forEach(child -> childResult.add(process(child, null)));
        return new FunctionExpression(
                functionExpression.getFunctionName().toLowerCase(),
                functionExpression.getFunctionAttributes(),
                childResult
        );
    }

    @Override
    public Expression visitTimeSeriesOperand(TimeSeriesOperand timeSeriesOperand, Void context) {
        PartialPath rawPath = timeSeriesOperand.getPath();
        if (rawPath.isMeasurementAliasExists()) {
            MeasurementPath measurementPath = (MeasurementPath) rawPath;
            MeasurementPath newPath =
                    new MeasurementPath(measurementPath, measurementPath.getMeasurementSchema());
            newPath.setUnderAlignedEntity(measurementPath.isUnderAlignedEntity());
            return new TimeSeriesOperand(newPath);
        }
        return timeSeriesOperand;
    }
}
