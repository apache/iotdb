package org.apache.iotdb.db.mpp.operator.meta;

import java.io.IOException;
import org.apache.iotdb.commons.partition.SchemaRegionId;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.metadata.schemaregion.SchemaRegion;
import org.apache.iotdb.db.mpp.operator.Operator;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

public class MetaScanOperator implements Operator {

  protected OperatorContext operatorContext;
  protected TsBlock tsBlock;

  protected SchemaRegion schemaRegion;
  protected int limit;
  protected int offset;
  protected PartialPath partialPath;
  protected boolean isPrefixPath;

  public MetaScanOperator(OperatorContext operatorContext, SchemaRegionId schemaRegionId,
      int limit, int offset, PartialPath partialPath, boolean isPrefixPath) {
    this.operatorContext = operatorContext;
    this.schemaRegion = SchemaEngine.getInstance().getSchemaRegion(schemaRegionId);
    this.limit = limit;
    this.offset = offset;
    this.partialPath = partialPath;
    this.isPrefixPath = isPrefixPath;
  }

  protected TsBlock createTsBlock() throws MetadataException {
    return null;
  }

  public PartialPath getPartialPath() {
    return partialPath;
  }

  public int getLimit() {
    return limit;
  }

  public int getOffset() {
    return offset;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public boolean isPrefixPath() {
    return isPrefixPath;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() throws IOException {
    return null;
  }

  @Override
  public boolean hasNext() throws IOException {
    return false;
  }
}
