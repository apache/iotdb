package org.apache.iotdb.db.mpp.plan.expression.visitor;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.constant.SqlConstant;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.LeafOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.cartesianProduct;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructFunctionExpressions;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructTimeSeriesOperands;
import static org.apache.iotdb.db.utils.TypeInferenceUtils.bindTypeForAggregationNonSeriesInputExpressions;

public class RemoveWildcardInFilterVisitor
    extends CartesianProductVisitor<RemoveWildcardInFilterVisitor.Context> {
  @Override
  public List<Expression> visitFunctionExpression(FunctionExpression predicate, Context context) {
    List<List<Expression>> extendedExpressions = new ArrayList<>();
    for (Expression suffixExpression : predicate.getExpressions()) {
      extendedExpressions.add(
          process(
              suffixExpression,
              new Context(context.getPrefixPaths(), context.getSchemaTree(), false)));

      // We just process first input Expression of AggregationFunction,
      // keep other input Expressions as origin and bind Type
      // If AggregationFunction need more than one input series,
      // we need to reconsider the process of it
      if (predicate.isBuiltInAggregationFunctionExpression()) {
        List<Expression> children = predicate.getExpressions();
        bindTypeForAggregationNonSeriesInputExpressions(
            predicate.getFunctionName(), children, extendedExpressions);
        break;
      }
    }
    List<List<Expression>> childExpressionsList = new ArrayList<>();
    cartesianProduct(extendedExpressions, childExpressionsList, 0, new ArrayList<>());
    return reconstructFunctionExpressions(predicate, childExpressionsList);
  }

  @Override
  public List<Expression> visitTimeSeriesOperand(TimeSeriesOperand predicate, Context context) {
    PartialPath filterPath = predicate.getPath();
    List<PartialPath> concatPaths = new ArrayList<>();
    if (!filterPath.getFirstNode().equals(SqlConstant.ROOT)) {
      context.getPrefixPaths().forEach(prefix -> concatPaths.add(prefix.concatPath(filterPath)));
    } else {
      // do nothing in the case of "where root.d1.s1 > 5"
      concatPaths.add(filterPath);
    }

    List<PartialPath> noStarPaths = new ArrayList<>();
    for (PartialPath concatPath : concatPaths) {
      List<MeasurementPath> actualPaths =
          context.getSchemaTree().searchMeasurementPaths(concatPath).left;
      if (actualPaths.isEmpty()) {
        return Collections.singletonList(new NullOperand());
      }
      noStarPaths.addAll(actualPaths);
    }
    return reconstructTimeSeriesOperands(noStarPaths);
  }

  @Override
  public List<Expression> visitLeafOperand(LeafOperand leafOperand, Context context) {
    return Collections.singletonList(leafOperand);
  }

  public static class Context {
    private final List<PartialPath> prefixPaths;
    private final ISchemaTree schemaTree;
    private final boolean isRoot;

    public Context(List<PartialPath> prefixPaths, ISchemaTree schemaTree, boolean isRoot) {
      this.prefixPaths = prefixPaths;
      this.schemaTree = schemaTree;
      this.isRoot = isRoot;
    }

    public List<PartialPath> getPrefixPaths() {
      return prefixPaths;
    }

    public ISchemaTree getSchemaTree() {
      return schemaTree;
    }

    public boolean isRoot() {
      return isRoot;
    }
  }
}
