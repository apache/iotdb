package org.apache.iotdb.db.metadata.rocksdb.mnode;

import org.apache.iotdb.db.engine.trigger.executor.TriggerExecutor;
import org.apache.iotdb.db.metadata.lastCache.container.ILastCacheContainer;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.Map;

public class RMeasurementMNode extends RMNode implements IMeasurementMNode {

  protected String alias;

  private IMeasurementSchema schema;

  private Map<String, String> tags;

  private Map<String, String> attributes;

  /**
   * Constructor of MNode.
   *
   * @param fullPath
   */
  public RMeasurementMNode(String fullPath) {
    super(fullPath);
  }

  @Override
  public boolean hasChild(String name) {
    return false;
  }

  @Override
  public IMNode getChild(String name) {
    return null;
  }

  @Override
  public Map<String, IMNode> getChildren() {
    return null;
  }

  @Override
  public boolean isStorageGroup() {
    return false;
  }

  @Override
  public boolean isEntity() {
    return false;
  }

  @Override
  public boolean isMeasurement() {
    return true;
  }

  @Override
  public void serializeTo(MLogWriter logWriter) throws IOException {}

  @Override
  public MeasurementPath getMeasurementPath() {
    return null;
  }

  @Override
  public IMeasurementSchema getSchema() {
    return schema;
  }

  @Override
  public TSDataType getDataType(String measurementId) {
    return schema.getType();
  }

  @Override
  public String getAlias() {
    return alias;
  }

  @Override
  public void setAlias(String alias) {
    this.alias = alias;
  }

  @Override
  public IEntityMNode getParent() {
    return null;
  }

  @Override
  public long getOffset() {
    return 0;
  }

  @Override
  public void setOffset(long offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TriggerExecutor getTriggerExecutor() {
    return null;
  }

  @Override
  public void setTriggerExecutor(TriggerExecutor triggerExecutor) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ILastCacheContainer getLastCacheContainer() {
    return null;
  }

  @Override
  public void setLastCacheContainer(ILastCacheContainer lastCacheContainer) {
    throw new UnsupportedOperationException();
  }
}
