/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tool.ui.node;

import org.apache.iotdb.tool.core.model.TimeSeriesMetadataNode;
import org.apache.iotdb.tool.ui.scene.IndexNodeInfoPage;
import org.apache.iotdb.tool.ui.scene.IoTDBParsePageV3;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;

import java.util.List;
import javafx.scene.Cursor;
import javafx.scene.Group;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.MenuItem;
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.StageStyle;

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

  private IoTDBParsePageV3 ioTDBParsePageV13;

  private boolean isLeafMeasurement;

  public EntityNode(
      IndexNode parent,
      TimeSeriesMetadataNode timeSeriesMetadataNode,
      int x,
      int y,
      boolean isLeafMeasurement,
      Group indexRegion,
      IoTDBParsePageV3 ioTDBParsePageV13) {

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
      Tooltip tooltip = new Tooltip(getTip());
      tooltip.setMaxWidth(IoTDBParsePageV3.WIDTH * 0.5);
      Tooltip.install(this.entityShape, tooltip);

      // show details in new stage
      MetadataIndexNodeType type = timeSeriesMetadataNode.getNodeType();
      switch (type) {
        case INTERNAL_DEVICE:
          addMenuToNode(entityShape, "InternalDevice Details");
          break;
        case LEAF_DEVICE:
          addMenuToNode(entityShape, "LeafDevice Details");
          break;
        case INTERNAL_MEASUREMENT:
          addMenuToNode(entityShape, "InternalMeasurement Details");
          break;
        case LEAF_MEASUREMENT:
          addMenuToNode(entityShape, "LeafMeasurement Details");
          break;
      }

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
        childButton.setOnMouseEntered(
            event -> {
              childButton.setCursor(Cursor.HAND);
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
    if (!this.childNode.isDraw()) {
      this.childNode.draw();
    } else {
      if (this.childNode != null) {
        this.childNode.setVisible(true);
      }
    }
    this.stretch = true;
  }

  private void shorten() {
    if (this.childNode != null) {
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

  private void addMenuToNode(TextField entityShape, String menuItemInfo) {
    ContextMenu contextMenu = new ContextMenu();
    MenuItem menuItem = new MenuItem(menuItemInfo);
    contextMenu.getItems().add(menuItem);
    entityShape.setContextMenu(contextMenu);
    menuItem.setOnAction(
        event -> {
          Stage pageInfoStage = new Stage();
          pageInfoStage.initStyle(StageStyle.UTILITY);
          pageInfoStage.initModality(Modality.APPLICATION_MODAL);
          String nodeInfo = getTip();
          IndexNodeInfoPage pageInfoPage =
              new IndexNodeInfoPage(pageInfoStage, menuItemInfo, nodeInfo);
        });
  }
}
