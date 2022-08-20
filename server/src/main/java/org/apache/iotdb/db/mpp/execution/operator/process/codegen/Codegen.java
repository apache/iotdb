package org.apache.iotdb.db.mpp.execution.operator.process.codegen;

import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

public interface Codegen {
  /** Accept inputs in Objects, return calculated value through code generation */
  Object[] accept(Object[] args, long timestamp) throws InvocationTargetException;

  /** construct expression to script, may fail and will return false */
  boolean addExpression(Expression expression);

  List<Boolean> isGenerated();

  Codegen setTypeProvider(TypeProvider typeProvider);

  /** this function should be called before {@link Codegen#addExpression(Expression)} */
  Codegen setInputs(List<String> paths, List<TSDataType> tsDataTypes);

  void generateScriptEvaluator() throws Exception;
}
