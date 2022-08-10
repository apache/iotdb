package org.apache.iotdb.tool.ui.node;

import javafx.scene.Group;
import javafx.scene.paint.Color;
import javafx.scene.shape.Line;
import javafx.scene.shape.Rectangle;
import org.apache.iotdb.tool.core.model.TimeSeriesMetadataNode;
import org.apache.iotdb.tool.ui.scene.IoTDBParsePageV13;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;

import java.util.ArrayList;
import java.util.List;

/**
 * index node wrap
 *
 * @author oortCLoudFei
 */
public class IndexNode {

  public static final int INDEX_ENTITY_SP_WIDTH = 10;
  public static final int INDEX_ENTITY_WIDTH = 100;
  public static final int INDEX_ENTITY_HEIGHT = 30;
  public static final int INDEX_LINE_HEIGHT = 80;

  private EntityNode parent;

  private TimeSeriesMetadataNode timeSeriesMetadataNode;

  private Group indexRegion;

  private boolean isLeafMeasurement = false;

  private Rectangle indexShape = null;

  private Line line = null;

  private List<EntityNode> children = new ArrayList<>(16);

  private int x;
  private int y;
  private int totalWidth;
  private int totalHeight;

  public IndexNode(
      TimeSeriesMetadataNode timeSeriesMetadataNode,
      EntityNode parent,
      Group indexRegion,
      IoTDBParsePageV13 ioTDBParsePageV13) {

    this.timeSeriesMetadataNode = timeSeriesMetadataNode;
    this.indexRegion = indexRegion;
    this.parent = parent;
    // default:root index
    int parentX = INDEX_ENTITY_SP_WIDTH;
    int parentY = INDEX_ENTITY_SP_WIDTH;
    if (parent != null) {
      parentX = parent.getX();
      parentY = parent.getY();
    }
    this.x = parentX;
    this.y = parentY + ((parent == null) ? 0 : INDEX_LINE_HEIGHT);
    this.totalWidth = INDEX_ENTITY_WIDTH + INDEX_ENTITY_SP_WIDTH + INDEX_ENTITY_SP_WIDTH;
    this.totalHeight = INDEX_ENTITY_HEIGHT + INDEX_ENTITY_SP_WIDTH * 2;
    List<TimeSeriesMetadataNode> tsmChildren = timeSeriesMetadataNode.getChildren();
    if (timeSeriesMetadataNode != null && tsmChildren != null && tsmChildren.size() > 0) {
      this.isLeafMeasurement =
          (MetadataIndexNodeType.LEAF_MEASUREMENT == timeSeriesMetadataNode.getNodeType());
      for (int i = 0; i < tsmChildren.size(); i++) {
        TimeSeriesMetadataNode child = tsmChildren.get(i);
        children.add(
            new EntityNode(
                this,
                child,
                this.x + INDEX_ENTITY_SP_WIDTH + (i * this.totalWidth),
                this.y + INDEX_ENTITY_SP_WIDTH,
                isLeafMeasurement,
                this.indexRegion,
                ioTDBParsePageV13));
      }
    }
  }

  public List<EntityNode> getChildren() {
    return children;
  }

  public void draw() {
    if (this.indexShape == null && this.indexRegion != null) {
      this.indexShape =
          new Rectangle(
              this.x,
              this.y,
              children.size() * this.totalWidth + INDEX_ENTITY_SP_WIDTH,
              this.totalHeight);
      this.indexShape.setFill(Color.GREY);
      if (parent != null) {
        this.line =
            new Line(
                this.x + IndexNode.INDEX_ENTITY_WIDTH / 2,
                this.y - INDEX_LINE_HEIGHT + INDEX_ENTITY_HEIGHT,
                this.x,
                this.y);
        this.indexRegion.getChildren().add(this.line);
      }
      this.indexRegion.getChildren().add(this.indexShape);
      children.forEach(entity -> entity.draw());
    }
  }

  public boolean isDraw() {
    return (this.indexShape != null);
  }

  public void setVisible(boolean visible) {
    if (isDraw()) {
      this.indexShape.setVisible(visible);
      if (children != null && children.size() > 0) {
        children.forEach(entity -> entity.setVisible(visible));
      }
      if (this.line != null) {
        this.line.setVisible(visible);
      }
    }
  }

  public int getX() {
    return x;
  }

  public int getY() {
    return y;
  }
}
