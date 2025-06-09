package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

public abstract class TreeModelIntoOperator extends AbstractIntoOperator {

  protected TreeModelIntoOperator(
      OperatorContext operatorContext,
      Operator child,
      List<TSDataType> inputColumnTypes,
      ExecutorService intoOperationExecutor,
      long statementSizePerLine) {
    super(operatorContext, child, inputColumnTypes, intoOperationExecutor, statementSizePerLine);
  }

  protected static List<InsertTabletStatementGenerator> constructInsertTabletStatementGenerators(
      Map<PartialPath, Map<String, InputLocation>> targetPathToSourceInputLocationMap,
      Map<PartialPath, Map<String, TSDataType>> targetPathToDataTypeMap,
      Map<String, Boolean> targetDeviceToAlignedMap,
      List<Type> sourceTypeConvertors,
      int maxRowNumberInStatement) {
    List<InsertTabletStatementGenerator> insertTabletStatementGenerators =
        new ArrayList<>(targetPathToSourceInputLocationMap.size());
    for (Map.Entry<PartialPath, Map<String, InputLocation>> entry :
        targetPathToSourceInputLocationMap.entrySet()) {
      PartialPath targetDevice = entry.getKey();
      TreeModelInsertTabletStatementGenerator generator =
          new TreeModelInsertTabletStatementGenerator(
              targetDevice,
              entry.getValue(),
              targetPathToDataTypeMap.get(targetDevice),
              targetDeviceToAlignedMap.get(targetDevice.toString()),
              sourceTypeConvertors,
              maxRowNumberInStatement);
      insertTabletStatementGenerators.add(generator);
    }
    return insertTabletStatementGenerators;
  }

  protected int findWritten(String device, String measurement) {
    for (InsertTabletStatementGenerator generator : insertTabletStatementGenerators) {
      if (!Objects.equals(generator.getDevice(), device)) {
        continue;
      }
      return generator.getWrittenCount(measurement);
    }
    return 0;
  }
}
