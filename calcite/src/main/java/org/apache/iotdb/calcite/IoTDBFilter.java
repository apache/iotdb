package org.apache.iotdb.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;

public class IoTDBFilter extends Filter implements IoTDBRel {
  private IExpression iExpression;

  protected IoTDBFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(cluster, traits, child, condition);

    this.iExpression = translateExpression((RexCall) condition);

    assert getConvention() == IoTDBRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  public IExpression getIExpression(){
    return this.iExpression;
  }

  private IExpression translateExpression(RexCall condition){
    IExpression expression = null;
    // 只处理了 time
    // 如果发现叶节点
    if (condition.getOperands().get(0) instanceof RexInputRef) {
      RexInputRef ref = (RexInputRef) condition.getOperands().get(0);
      int columnIndex = ref.getIndex();
      if(columnIndex == 0){
        RexLiteral literal = (RexLiteral)condition.getOperands().get(1);
        long value = (long) literal.getValue2();
        SqlKind kind = condition.getOperator().getKind();
        // BasicOperatorType funcToken = getBasicOpType(kind);
        expression = getGlobalTimeExpression(kind, value);
      }
    } else {
      SqlKind kind = condition.getOperator().getKind();
      RexCall left = (RexCall) condition.getOperands().get(0);
      if(kind == SqlKind.AND){
        RexCall right = (RexCall) condition.getOperands().get(1);
        expression = BinaryExpression.and(translateExpression(left), translateExpression(right));
      }
      else if(kind == SqlKind.OR){
        RexCall right = (RexCall) condition.getOperands().get(1);
        expression = BinaryExpression.or(translateExpression(left), translateExpression(right));
      }
      else if(kind == SqlKind.NOT){
        // ???
        org.apache.iotdb.tsfile.read.filter.basic.Filter filter = ((GlobalTimeExpression)translateExpression(left)).getFilter();
        TimeFilter.TimeNotFilter timeNotFilter = TimeFilter.not(filter);
        expression = new GlobalTimeExpression(timeNotFilter);
      }
    }
    return expression;
  }

/*  private BasicOperatorType getBasicOpType(SqlKind kind) {
    if(kind == SqlKind.EQUALS){
      return BasicOperatorType.EQ;
    }
    else if(kind == SqlKind.LESS_THAN){
      return BasicOperatorType.LT;
    }
    else if(kind == SqlKind.LESS_THAN_OR_EQUAL){
      return BasicOperatorType.LTEQ;
    }
    else if(kind == SqlKind.GREATER_THAN){
      return BasicOperatorType.GT;
    }
    else if(kind == SqlKind.GREATER_THAN_OR_EQUAL){
      return BasicOperatorType.GTEQ;
    }
    else if(kind == SqlKind.NOT_EQUALS){
      return BasicOperatorType.NOTEQUAL;
    }
    else
      return null;
  }*/

  private GlobalTimeExpression getGlobalTimeExpression(SqlKind kind, long value){
    GlobalTimeExpression expression = null;
    if(kind == SqlKind.EQUALS){
      TimeFilter.TimeEq timeEq = TimeFilter.eq(value);
      expression = new GlobalTimeExpression(timeEq);
    }
    else if(kind == SqlKind.LESS_THAN){
      TimeFilter.TimeLt timeLt = TimeFilter.lt(value);
      expression = new GlobalTimeExpression(timeLt);
    }
    else if(kind == SqlKind.LESS_THAN_OR_EQUAL){
      TimeFilter.TimeLtEq timeLtEq = TimeFilter.ltEq(value);
      expression = new GlobalTimeExpression(timeLtEq);
    }
    else if(kind == SqlKind.GREATER_THAN){
      TimeFilter.TimeGt timeGt = TimeFilter.gt(value);
      expression = new GlobalTimeExpression(timeGt);
    }
    else if(kind == SqlKind.GREATER_THAN_OR_EQUAL){
      TimeFilter.TimeGtEq timeGtEq = TimeFilter.gtEq(value);
      expression = new GlobalTimeExpression(timeGtEq);
    }
    else if(kind == SqlKind.NOT_EQUALS){
      TimeFilter.TimeNotEq timeNotEq = TimeFilter.notEq(value);
      expression = new GlobalTimeExpression(timeNotEq);
    }

    return expression;
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
                                              RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override
  public Filter copy(RelTraitSet relTraitSet, RelNode relNode, RexNode rexNode) {
    return new IoTDBFilter(getCluster(), traitSet, input, condition);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
  }
}
