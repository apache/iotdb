package org.apache.iotdb.db.mpp.plan.expression.visitor;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.LeafOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;

import java.util.Collections;
import java.util.List;

public class CollectSourceExpressionsVisitor extends CollectVisitor {
    @Override
    public List<Expression> visitFunctionExpression(FunctionExpression functionExpression, Void context) {
        return collectFromChild(functionExpression);
    }

    @Override
    public List<Expression> visitTimeSeriesOperand(TimeSeriesOperand timeSeriesOperand, Void context) {
        return Collections.singletonList(timeSeriesOperand);
    }

    @Override
    public List<Expression> visitLeafOperand(LeafOperand leafOperand, Void context) {
        return Collections.emptyList();
    }
}
