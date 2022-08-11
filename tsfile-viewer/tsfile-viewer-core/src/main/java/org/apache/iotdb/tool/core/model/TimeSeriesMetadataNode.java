package org.apache.iotdb.tool.core.model;

import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;

import java.util.ArrayList;
import java.util.List;

public class TimeSeriesMetadataNode {

  private List<TimeSeriesMetadataNode> children = new ArrayList<>();

  private MetadataIndexNodeType nodeType;

  private String deviceId;

  private String measurementId;

  private long position;

  private ITimeSeriesMetadata timeseriesMetadata;

  private boolean aligned;

  public TimeSeriesMetadataNode() {}

  public TimeSeriesMetadataNode(
      List<TimeSeriesMetadataNode> children,
      MetadataIndexNodeType nodeType,
      String deviceId,
      String measurementId,
      TimeseriesMetadata timeseriesMetadata) {
    this.children = children;
    this.nodeType = nodeType;
    this.deviceId = deviceId;
    this.measurementId = measurementId;
    this.timeseriesMetadata = timeseriesMetadata;
  }

  public List<TimeSeriesMetadataNode> getChildren() {
    return children;
  }

  public void setChildren(List<TimeSeriesMetadataNode> children) {
    this.children = children;
  }

  public String getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public String getMeasurementId() {
    return measurementId;
  }

  public void setMeasurementId(String measurementId) {
    this.measurementId = measurementId;
  }

  public long getPosition() {
    return position;
  }

  public void setPosition(long position) {
    this.position = position;
  }

  public ITimeSeriesMetadata getTimeseriesMetadata() {
    return timeseriesMetadata;
  }

  public void setTimeseriesMetadata(ITimeSeriesMetadata timeseriesMetadata) {
    this.timeseriesMetadata = timeseriesMetadata;
  }

  public MetadataIndexNodeType getNodeType() {
    return nodeType;
  }

  public void setNodeType(MetadataIndexNodeType nodeType) {
    this.nodeType = nodeType;
  }

  public boolean isAligned() {
    return aligned;
  }

  public void setAligned(boolean aligned) {
    this.aligned = aligned;
  }
}
