package org.apache.iotdb.db.mpp.plan.expression.visitor;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.analyze.GroupByLevelController;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;

import java.util.ArrayList;
import java.util.List;

//import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionAnalyzer.replaceRawPathWithGroupedPath;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructFunctionExpression;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructTimeSeriesOperand;

public class ReplaceRawPathWithGroupedPathVisitor extends ReconstructVisitor<GroupByLevelController.RawPathToGroupedPathMap> {
    @Override
    public Expression visitFunctionExpression(FunctionExpression functionExpression, GroupByLevelController.RawPathToGroupedPathMap rawPathToGroupedPathMap) {
        List<Expression> childrenExpressions = new ArrayList<>();
        for (Expression childExpression : functionExpression.getExpressions()) {
            childrenExpressions.add(
                    process(childExpression, rawPathToGroupedPathMap));

            // We just process first input Expression of AggregationFunction.
            // If AggregationFunction need more than one input series,
            // we need to reconsider the process of it
            if (functionExpression.isBuiltInAggregationFunctionExpression()) {
                List<Expression> children = functionExpression.getExpressions();
                for (int i = 1; i < children.size(); i++) {
                    childrenExpressions.add(children.get(i));
                }
                break;
            }
        }
        return reconstructFunctionExpression((FunctionExpression) functionExpression, childrenExpressions);
    }

    @Override
    public Expression visitTimeSeriesOperand(TimeSeriesOperand timeSeriesOperand, GroupByLevelController.RawPathToGroupedPathMap rawPathToGroupedPathMap) {
        PartialPath rawPath = timeSeriesOperand.getPath();
        PartialPath groupedPath = rawPathToGroupedPathMap.get(rawPath);
        return reconstructTimeSeriesOperand(groupedPath);
    }

    @Override
    public Expression visitTimeStampOperand(TimestampOperand timestampOperand, GroupByLevelController.RawPathToGroupedPathMap context) {
        return timestampOperand;
    }

    @Override
    public Expression visitConstantOperand(ConstantOperand constantOperand, GroupByLevelController.RawPathToGroupedPathMap context) {
        return constantOperand;
    }
}
