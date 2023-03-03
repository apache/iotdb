package org.apache.iotdb.db.mpp.plan.expression.visitor;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.LeafOperand;
import org.apache.iotdb.db.mpp.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.UnaryExpression;

import java.util.ArrayList;
import java.util.List;

/**
 * Simply collects result from child-expression.
 * For example, two child give me 3 and 4 results,
 * I will return 3+4 = 7 results to upper level.
 */
public class CollectVisitor extends ExpressionAnalyzeVisitor<List<Expression>> {
    protected List<Expression> collectFromChild(Expression expression) {
        List<Expression> result = new ArrayList<>();
        for (Expression child : expression.getExpressions())
            result.addAll(process(child,null));
        return result;
    }

    @Override
    public List<Expression> visitTernaryExpression(TernaryExpression ternaryExpression, Void context) {
        return collectFromChild(ternaryExpression);
    }

    @Override
    public List<Expression> visitBinaryExpression(BinaryExpression binaryExpression, Void context) {
        return collectFromChild(binaryExpression);
    }

    @Override
    public List<Expression> visitUnaryExpression(UnaryExpression unaryExpression, Void context) {
        return collectFromChild(unaryExpression);
    }

    @Override
    public List<Expression> visitLeafOperand(LeafOperand leafOperand, Void context) {
        return super.visitLeafOperand(leafOperand, context);
    }
}
