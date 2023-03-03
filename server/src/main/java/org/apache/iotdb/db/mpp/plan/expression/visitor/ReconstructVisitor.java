package org.apache.iotdb.db.mpp.plan.expression.visitor;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.TernaryExpression;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructAllExpression;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructBinaryExpression;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructFunctionExpression;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructTernaryExpression;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructTimeSeriesOperand;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructUnaryExpression;

/**
 * Collect result from child, then reconstruct.
 * For example, two child each give me 1 result,
 * I should use them to reconstruct 1 new result to upper level.
 */
public class ReconstructVisitor extends ExpressionAnalyzeVisitor<Expression> {
    private Expression collectFromChild(Expression expression) {
        List<Expression> childResult = new ArrayList<>();
        expression.getExpressions()
                .forEach(child -> childResult.add(process(child, null)));
        return reconstructAllExpression(expression, childResult);
    }


}
