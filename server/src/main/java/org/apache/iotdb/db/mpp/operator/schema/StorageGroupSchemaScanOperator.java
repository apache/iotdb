package org.apache.iotdb.db.mpp.operator.schema;

import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.db.mpp.operator.source.SourceOperator;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.Collections;
import java.util.List;

public class StorageGroupSchemaScanOperator implements SourceOperator {
  private final List<String> storageGroups;
  private final PlanNodeId sourceId;
  private final OperatorContext operatorContext;

  private final TsBlock tsBlock;

  private boolean hasNext = true;

  public StorageGroupSchemaScanOperator(
      PlanNodeId sourceId, OperatorContext operatorContext, List<String> storageGroups) {
    this.sourceId = sourceId;
    this.operatorContext = operatorContext;
    this.storageGroups = storageGroups;
    tsBlock = createTsBlock();
  }

  private TsBlock createTsBlock() {
    if (storageGroups.isEmpty()) {
      hasNext = false;
      return null;
    }
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.TEXT));
    builder.getTimeColumnBuilder().writeLong(0L);
    storageGroups.forEach(sg -> builder.getColumnBuilder(0).writeBinary(new Binary(sg)));
    return builder.build();
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    hasNext = false;
    return tsBlock;
  }

  @Override
  public boolean hasNext() {
    return hasNext;
  }

  @Override
  public boolean isFinished() {
    return !hasNext;
  }
}
