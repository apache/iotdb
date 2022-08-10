package org.apache.iotdb.tool.ui.node;

import javafx.scene.Group;
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;
import org.apache.iotdb.tool.core.model.TimeSeriesMetadataNode;
import org.apache.iotdb.tool.ui.scene.IoTDBParsePageV13;

import java.util.List;

/**
 * index entity node
 *
 * @author oortCloudFei
 */
public class EntityNode {

  private IndexNode parent;

  private TimeSeriesMetadataNode timeSeriesMetadataNode;

  private Group indexRegion;

  private TextField entityShape = null;

  private boolean stretch = false;

  private Circle childButton = null;

  private IndexNode childNode;

  private int x;
  private int y;

  private IoTDBParsePageV13 ioTDBParsePageV13;

  private boolean isLeafMeasurement;

  public EntityNode(
      IndexNode parent,
      TimeSeriesMetadataNode timeSeriesMetadataNode,
      int x,
      int y,
      boolean isLeafMeasurement,
      Group indexRegion,
      IoTDBParsePageV13 ioTDBParsePageV13) {

    this.parent = parent;
    this.timeSeriesMetadataNode = timeSeriesMetadataNode;
    this.x = x;
    this.y = y;
    this.isLeafMeasurement = isLeafMeasurement;
    this.indexRegion = indexRegion;
    this.ioTDBParsePageV13 = ioTDBParsePageV13;
    this.childNode = new IndexNode(timeSeriesMetadataNode, this, indexRegion, ioTDBParsePageV13);
  }

  public IndexNode getParent() {
    return parent;
  }

  public synchronized void draw() {

    if (this.entityShape == null && this.indexRegion != null) {
      String measurementId = this.timeSeriesMetadataNode.getMeasurementId();
      String deviceId = this.timeSeriesMetadataNode.getDeviceId();
      String showName =
          measurementId != null ? measurementId : (deviceId == null ? "root" : deviceId);
      this.entityShape = new TextField(showName);
      this.entityShape.setEditable(false);
      this.entityShape.setLayoutX(this.x);
      this.entityShape.setLayoutY(this.y);
      this.entityShape.setMaxWidth(IndexNode.INDEX_ENTITY_WIDTH);
      this.entityShape.setMinWidth(IndexNode.INDEX_ENTITY_WIDTH);
      this.entityShape.setMinHeight(IndexNode.INDEX_ENTITY_HEIGHT);
      this.entityShape.setMaxHeight(IndexNode.INDEX_ENTITY_HEIGHT);
      // link the tree
      this.entityShape.setOnMouseClicked(
          e -> {
            if (deviceId == null) {
              return;
            }
            // find by path
            String path = deviceId + (measurementId == null ? "" : "." + measurementId);
            this.ioTDBParsePageV13.chooseTree(path);
          });
      // tips,show detail
      Tooltip.install(this.entityShape, new Tooltip(getTip()));
      this.indexRegion.getChildren().add(this.entityShape);
      if (!this.isLeafMeasurement) {
        this.childButton =
            new Circle(
                this.x + IndexNode.INDEX_ENTITY_WIDTH / 2,
                this.y + IndexNode.INDEX_ENTITY_HEIGHT,
                5,
                Color.BLACK);
        this.indexRegion.getChildren().add(childButton);
        childButton.setOnMouseClicked(
            e -> {
              if (stretch) {
                stretch = false;
                shorten();
              } else {
                // show children
                stretch = true;
                stretch();
              }
            });
      }
    }
  }

  private String getTip() {

    StringBuilder sb = new StringBuilder();
    sb.append("type:").append(this.timeSeriesMetadataNode.getNodeType().toString());
    sb.append("\n");
    sb.append("position:").append(this.timeSeriesMetadataNode.getPosition());
    sb.append("\n");
    switch (this.timeSeriesMetadataNode.getNodeType()) {
      case LEAF_DEVICE:
        sb.append("deviceId:");
        sb.append(this.timeSeriesMetadataNode.getDeviceId());
        break;
      case LEAF_MEASUREMENT:
        sb.append(
            "measurementId:"
                + this.timeSeriesMetadataNode.getMeasurementId()
                + ",statistics:"
                + this.timeSeriesMetadataNode.getTimeseriesMetadata());
        break;
      default:
        break;
    }
    return sb.toString();
  }

  private void stretch() {
    closeBrotherNode(this);
    if(!this.childNode.isDraw()) {
      this.childNode.draw();
    } else {
      if(this.childNode != null) {
        this.childNode.setVisible(true);
      }
    }
    this.stretch = true;
  }

  private void shorten() {
    if(this.childNode != null) {
      this.childNode.setVisible(false);
      this.stretch = false;
    }
  }

  public boolean isDraw() {
    return (this.entityShape != null);
  }

  public void setVisible(boolean visible) {

    if (isDraw()) {
      this.entityShape.setVisible(visible);
      if (this.childButton != null) {
        this.childButton.setVisible(visible);
      }
      if (this.stretch && this.childNode != null && !this.isLeafMeasurement) {
        this.childNode.setVisible(visible);
      }
    }
  }

  public int getX() {
    return x;
  }

  public int getY() {
    return y;
  }

  private void closeBrotherNode(EntityNode node) {
    IndexNode parent = node.parent;
    List<EntityNode> brothers = parent.getChildren();
    for (EntityNode brother : brothers) {
      if (brother != node && brother.stretch) {
        brother.shorten();
        break;
      }
    }
  }
}
