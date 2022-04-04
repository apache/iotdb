package org.apache.iotdb.db.mpp.operator.meta;

import org.apache.iotdb.commons.partition.SchemaRegionId;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.metadata.schemaregion.SchemaRegion;
import org.apache.iotdb.db.mpp.operator.Operator;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import java.io.IOException;
import java.util.List;

public class MetaScanOperator implements Operator {

  protected OperatorContext operatorContext;
  protected TsBlock tsBlock;
  private boolean hasCachedTsBlock;

  protected SchemaRegion schemaRegion;
  protected int limit;
  protected int offset;
  protected PartialPath partialPath;
  protected boolean isPrefixPath;
  protected List<String> columns;

  public MetaScanOperator(
      OperatorContext operatorContext,
      SchemaRegionId schemaRegionId,
      int limit,
      int offset,
      PartialPath partialPath,
      boolean isPrefixPath,
      List<String> columns) {
    this.operatorContext = operatorContext;
    this.schemaRegion = SchemaEngine.getInstance().getSchemaRegion(schemaRegionId);
    this.limit = limit;
    this.offset = offset;
    this.partialPath = partialPath;
    this.isPrefixPath = isPrefixPath;
    this.columns = columns;
  }

  protected TsBlock createTsBlock() throws MetadataException {
    return null;
  }

  public PartialPath getPartialPath() {
    return partialPath;
  }

  public SchemaRegion getSchemaRegion() {
    return schemaRegion;
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
    hasCachedTsBlock = false;
    return tsBlock;
  }

  @Override
  public boolean hasNext() throws IOException {
    try {
      if (tsBlock == null && !hasCachedTsBlock) {
        tsBlock = createTsBlock();
        hasCachedTsBlock = true;
      }
      return tsBlock != null && tsBlock.getPositionCount() > 0;
    } catch (MetadataException e) {
      throw new IOException(e);
    }
  }
}
