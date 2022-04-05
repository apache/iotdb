package org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeIdAllocator;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_ATTRIBUTES;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_STORAGE_GROUP;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TAGS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_ALIAS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_DATATYPE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_ENCODING;

public class TimeSeriesMetaScanNode extends MetaScanNode {

  private String key;
  private String value;
  private boolean isContains;

  // if is true, the result will be sorted according to the inserting frequency of the timeseries
  private boolean orderByHeat;

  public TimeSeriesMetaScanNode(
      PlanNodeId id,
      PartialPath partialPath,
      String key,
      String value,
      int limit,
      int offset,
      boolean orderByHeat,
      boolean isContains,
      boolean isPrefixPath) {
    super(id, partialPath, limit, offset, isPrefixPath);
    this.key = key;
    this.value = value;
    this.orderByHeat = orderByHeat;
    this.isContains = isContains;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    PlanNodeType.TIME_SERIES_META_SCAN.serialize(byteBuffer);
    ReadWriteIOUtils.write(getId().getId(), byteBuffer);
    ReadWriteIOUtils.write(path.getFullPath(), byteBuffer);
    ReadWriteIOUtils.write(key, byteBuffer);
    ReadWriteIOUtils.write(value, byteBuffer);
    ReadWriteIOUtils.write(limit, byteBuffer);
    ReadWriteIOUtils.write(offset, byteBuffer);
    ReadWriteIOUtils.write(orderByHeat, byteBuffer);
    ReadWriteIOUtils.write(isContains, byteBuffer);
    ReadWriteIOUtils.write(isPrefixPath(), byteBuffer);
  }

  public static TimeSeriesMetaScanNode deserialize(ByteBuffer byteBuffer)
      throws IllegalPathException {
    String id = ReadWriteIOUtils.readString(byteBuffer);
    PlanNodeId planNodeId = new PlanNodeId(id);
    String fullPath = ReadWriteIOUtils.readString(byteBuffer);
    PartialPath path = new PartialPath(fullPath);
    String key = ReadWriteIOUtils.readString(byteBuffer);
    String value = ReadWriteIOUtils.readString(byteBuffer);
    int limit = ReadWriteIOUtils.readInt(byteBuffer);
    int offset = ReadWriteIOUtils.readInt(byteBuffer);
    boolean oderByHeat = ReadWriteIOUtils.readBool(byteBuffer);
    boolean isContains = ReadWriteIOUtils.readBool(byteBuffer);
    boolean isPrefixPath = ReadWriteIOUtils.readBool(byteBuffer);
    return new TimeSeriesMetaScanNode(
        planNodeId, path, key, value, limit, offset, oderByHeat, isContains, isPrefixPath);
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public boolean isContains() {
    return isContains;
  }

  public void setContains(boolean contains) {
    isContains = contains;
  }

  public boolean isOrderByHeat() {
    return orderByHeat;
  }

  public void setOrderByHeat(boolean orderByHeat) {
    this.orderByHeat = orderByHeat;
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new TimeSeriesMetaScanNode(
        PlanNodeIdAllocator.generateId(),
        path,
        key,
        value,
        limit,
        offset,
        orderByHeat,
        isContains,
        isPrefixPath());
  }

  @Override
  public List<String> getOutputColumnNames() {
    return Arrays.asList(
        COLUMN_TIMESERIES,
        COLUMN_TIMESERIES_ALIAS,
        COLUMN_STORAGE_GROUP,
        COLUMN_TIMESERIES_DATATYPE,
        COLUMN_TIMESERIES_ENCODING,
        COLUMN_TIMESERIES_COMPRESSION,
        COLUMN_TAGS,
        COLUMN_ATTRIBUTES);
  }
}
