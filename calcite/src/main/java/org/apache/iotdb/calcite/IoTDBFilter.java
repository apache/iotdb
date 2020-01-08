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
import org.apache.calcite.util.Util;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.strategy.optimizer.DnfFilterOptimizer;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.*;

public class IoTDBFilter extends Filter implements IoTDBRel {
  private final FilterOperator filterOperator;
  private final List<String> fieldNames;
  private List<String> predicates;
  private boolean flag = true; // to check if devices can be added now
  List<String> devices = new ArrayList<>();

  protected IoTDBFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition)
          throws LogicalOptimizeException {
    super(cluster, traits, child, condition);
    this.fieldNames = IoTDBRules.IoTDBFieldNames(getRowType());
    this.filterOperator = getIoTDBOperator(condition);
    this.predicates = translateWhere(filterOperator);

    assert getConvention() == IoTDBRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override
  public Filter copy(RelTraitSet relTraitSet, RelNode input, RexNode condition) {
    try {
      return new IoTDBFilter(getCluster(), traitSet, input, condition);
    } catch (LogicalOptimizeException e) {
      throw new AssertionError(e.getMessage());
    }
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    implementor.add(devices, predicates);
  }

  /** Translates {@link RexNode} expressions into IoTDB filter operator. */
  private FilterOperator getIoTDBOperator(RexNode filter){
    switch (filter.getKind()){
      case EQUALS:
        return getBasicOperator(SQLConstant.EQUAL, (RexCall) filter);
      case NOT_EQUALS:
        return getBasicOperator(SQLConstant.NOTEQUAL, (RexCall) filter);
      case GREATER_THAN:
        return getBasicOperator(SQLConstant.GREATERTHAN, (RexCall) filter);
      case GREATER_THAN_OR_EQUAL:
        return getBasicOperator(SQLConstant.GREATERTHANOREQUALTO, (RexCall) filter);
      case LESS_THAN:
        return getBasicOperator(SQLConstant.LESSTHAN, (RexCall) filter);
      case LESS_THAN_OR_EQUAL:
        return getBasicOperator(SQLConstant.LESSTHANOREQUALTO, (RexCall) filter);
      case AND:
        return getBinaryOperator(SQLConstant.KW_AND, ((RexCall) filter).getOperands());
      case OR:
        return getBinaryOperator(SQLConstant.KW_OR, ((RexCall) filter).getOperands());
      default:
        return null;
    }
  }

  private FilterOperator getBasicOperator(int tokenIntType, RexCall call){
    final RexNode left = call.operands.get(0);
    final RexNode right = call.operands.get(1);

    FilterOperator operator = getBasicOperator2(tokenIntType, left, right);
    if(operator != null){
      return operator;
    }
    operator = getBasicOperator2(tokenIntType, right, left);
    if(operator != null){
      return operator;
    }
    throw new AssertionError("cannnot translate basic operator: " + call);
  }

  private FilterOperator getBasicOperator2(int tokenIntType, RexNode left, RexNode right){
    switch (right.getKind()) {
      case LITERAL:
        break;
      default:
        return null;
    }
    final RexLiteral rightLiteral = (RexLiteral) right;
    switch (left.getKind()) {
      case INPUT_REF:
        String name = fieldNames.get(((RexInputRef) left).getIndex());
        return new BasicFunctionOperator(tokenIntType, new Path(name), literalValue(rightLiteral));
      default:
        return null;
    }
  }

  private FilterOperator getBinaryOperator(int tokenIntType, List<RexNode> operands){
    FilterOperator filterBinaryTree = new FilterOperator(tokenIntType);
    FilterOperator currentNode = filterBinaryTree;
    for (int i = 0; i < operands.size(); i++) {
      if (i > 0 && i < operands.size() - 1) {
        FilterOperator newInnerNode = new FilterOperator(tokenIntType);
        currentNode.addChildOperator(newInnerNode);
        currentNode = newInnerNode;
      }
      currentNode.addChildOperator(getIoTDBOperator(operands.get(i)));
    }
    return filterBinaryTree;
  }

  /** Produce the IoTDB predicate string for the given operator.
   *
   * @param operator Condition to translate
   * @return IoTDB predicate string
   */
  private List<String> translateWhere(FilterOperator operator) throws LogicalOptimizeException {
    List<String> whereClause = new ArrayList<>();
    if(operator.isLeaf()){
      if(translateLeaf((BasicFunctionOperator) operator) != null){
        whereClause.add(translateLeaf((BasicFunctionOperator) operator));
        return whereClause;
      } else {
        return whereClause;
      }
    }
    DnfFilterOptimizer dnfFilterOptimizer = new DnfFilterOptimizer();
    FilterOperator dnfOperator = dnfFilterOptimizer.optimize(operator);

    // get conjunction children in disjunction normal form
    List<FilterOperator> children = dnfOperator.getChildren();
    for (FilterOperator child : children) {
      String childAnd = translateAnd(child);
      if(childAnd != null){
        whereClause.add(childAnd);
      }
    }

    return whereClause;
  }

  private String translateLeaf(BasicFunctionOperator operator){
    if(operator.getPath().equals(IoTDBConstant.DeviceColumn)){
      if(flag){
        this.devices.add(operator.getValue());
      }
      return null;
    } else {
      return translateBasicOperator(null, operator);
    }
  }

  /**
   * Translate a conjunction predicate to a IoTDB expression string.
   *
   * @param operator A conjunctive predicate
   * @return IoTDB where clause string for the predicate
   */
  private String translateAnd(FilterOperator operator){
    if(operator.isLeaf()){
      return translateLeaf((BasicFunctionOperator) operator);
    }
    List<String> predicates = new ArrayList<>();
    String deviceName = null;
    // e.g. Device = d1 AND Time > 15 AND s0 > 5
    List<FilterOperator> children = operator.getChildren();
    Iterator<FilterOperator> iter = children.iterator();

    while(iter.hasNext()) {
      FilterOperator child = iter.next();
      if(child instanceof BasicFunctionOperator
        && ((BasicFunctionOperator) child).getPath().equals(IoTDBConstant.DeviceColumn)){

        String device = ((BasicFunctionOperator) child).getValue();
        // e.g. Device = d1 AND Device = d2
        if(deviceName != null && !deviceName.equals(device)){
          throw new AssertionError("Wrong restrictions to device: " + deviceName + " AND " + device);
        } else if(deviceName == null){
          deviceName = device;
        }
        iter.remove();
      }
    }
    // if a conjunction has no device restriction which means there is a global restriction,
    // the devices in from clause will be '*', and don't allow to add deviceName again.
    if (deviceName == null){
      this.devices = new ArrayList<>();
      this.flag = false;
    } else if (flag) {
      this.devices.add(deviceName);
    }

    for (FilterOperator child : children) {
      if(child instanceof BasicFunctionOperator){
        predicates.add(translateBasicOperator(deviceName, (BasicFunctionOperator) child));
      } else {
        throw new AssertionError( "cannot translate" + child.toString());
      }
    }

    return Util.toString(predicates, "", " AND ", "");
  }

  private String translateBasicOperator(String device, BasicFunctionOperator operator){
    StringBuilder buf = new StringBuilder();
    if(device != null && !operator.getPath().equals(IoTDBConstant.TimeColumn)){
      buf.append(device + IoTDBConstant.PATH_SEPARATOR);
    }
    buf.append(operator.getPath());

    switch(operator.getTokenIntType()){
      case SQLConstant.EQUAL:
        return buf.append("=").append(operator.getValue()).toString();
      case SQLConstant.NOTEQUAL:
        return buf.append("!=").append(operator.getValue()).toString();
      case SQLConstant.GREATERTHAN:
        return buf.append(">").append(operator.getValue()).toString();
      case SQLConstant.GREATERTHANOREQUALTO:
        return buf.append(">=").append(operator.getValue()).toString();
      case SQLConstant.LESSTHAN:
        return buf.append("<").append(operator.getValue()).toString();
      case SQLConstant.LESSTHANOREQUALTO:
        return buf.append("<=").append(operator.getValue()).toString();
      default:
        throw new AssertionError("cannot translate " + operator);
    }
  }

  /** Convert the value of a literal to a string.
   *
   * @param literal Literal to translate
   * @return String representation of the literal
   */
  private static String literalValue(RexLiteral literal) {
    Object value = literal.getValue2();
    StringBuilder buf = new StringBuilder();
    buf.append(value);
    return buf.toString();
  }

}
