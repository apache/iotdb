package org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern;

import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression.ArithmeticOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression.BinaryComputation;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression.ComparisonOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression.Computation;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression.ReferenceComputation;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PatternExpressionTest {

  @Test
  public void testComplexExpression() {
    // 表达式: (#0 + #1) < #2
    Computation addition =
        new BinaryComputation(
            new ReferenceComputation(0), new ReferenceComputation(1), ArithmeticOperator.ADD);

    Computation fullExpression =
        new BinaryComputation(addition, new ReferenceComputation(2), ComparisonOperator.LESS_THAN);

    List<Object> values = Arrays.asList(3, 4, 10); // 3 + 4 < 10 -> true
    assertEquals(true, fullExpression.evaluate(values));

    values = Arrays.asList(6, 5, 10); // 6 + 5 < 10 -> false
    assertEquals(false, fullExpression.evaluate(values));
  }
}
