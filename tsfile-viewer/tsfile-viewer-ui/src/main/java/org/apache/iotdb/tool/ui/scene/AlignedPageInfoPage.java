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

package org.apache.iotdb.tool.ui.scene;

import org.apache.iotdb.tool.core.model.IPageInfo;
import org.apache.iotdb.tsfile.read.common.BatchData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.Scene;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TreeItem;
import javafx.scene.control.cell.MapValueFactory;
import javafx.scene.layout.AnchorPane;
import javafx.stage.Stage;

import static org.apache.iotdb.tool.ui.common.constant.StageConstant.ALIGNED_PAGE_INFO_PAGE_HEIGHT;
import static org.apache.iotdb.tool.ui.common.constant.StageConstant.ALIGNED_PAGE_INFO_PAGE_WIDTH;

/**
 * AlignedPageInfoPage
 *
 * @author shenguanchu
 */
public class AlignedPageInfoPage {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBParsePageV3.class);

  private static final String TIMESTAMP_COLUMN = "timestamp";
  private static final String VALUE_COLUMN = "value";

  private AnchorPane anchorPane;
  private Scene scene;
  private IoTDBParsePageV3 ioTDBParsePage;
  private Stage stage;

  private AnchorPane pageDataPane;

  private TreeItem<IoTDBParsePageV3.ChunkTreeItemValue> pageItem;

  public AlignedPageInfoPage() {}

  public AlignedPageInfoPage(
      Stage stage,
      IoTDBParsePageV3 ioTDBParsePage,
      TreeItem<IoTDBParsePageV3.ChunkTreeItemValue> pageItem) {
    this.stage = stage;
    this.ioTDBParsePage = ioTDBParsePage;
    this.pageItem = pageItem;
    init(stage);
  }

  public Scene getScene() {
    return scene;
  }

  private void init(Stage stage) {
    anchorPane = new AnchorPane();
    scene = new Scene(anchorPane, ALIGNED_PAGE_INFO_PAGE_WIDTH, ALIGNED_PAGE_INFO_PAGE_HEIGHT);
    stage.setScene(scene);
    stage.setTitle("Aligned Page Information");
    stage.show();
    stage.setResizable(false);

    // Table Data Source
    ObservableList<HashMap<String, SimpleStringProperty>> columnDataList =
        FXCollections.observableArrayList();
    // Table Init
    TableView<HashMap<String, SimpleStringProperty>> alignedTableView =
        new TableView<HashMap<String, SimpleStringProperty>>(columnDataList);
    pageDataPane = new AnchorPane();
    pageDataPane.setLayoutX(0);
    pageDataPane.setLayoutY(0);
    pageDataPane.setPrefHeight(ALIGNED_PAGE_INFO_PAGE_WIDTH);
    anchorPane.getChildren().add(pageDataPane);
    pageDataPane.getChildren().add(alignedTableView);
    alignedTableView.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
    alignedTableView.setVisible(true);
    // Add Column and Data to TableView
    IoTDBParsePageV3.AlignedPageItemParams pageItemParams =
        (IoTDBParsePageV3.AlignedPageItemParams) pageItem.getValue().getParams();
    IPageInfo pageInfo = pageItemParams.getPageInfoList();
    try {
      BatchData batchData =
          ioTDBParsePage.getTsFileAnalyserV13().fetchBatchDataByPageInfo(pageInfo);
      // 1. Add Time Column and it's Data
      TableColumn<HashMap<String, SimpleStringProperty>, String> timestampCol =
          new TableColumn<HashMap<String, SimpleStringProperty>, String>(TIMESTAMP_COLUMN);
      alignedTableView.getColumns().add(timestampCol);
      timestampCol.setCellValueFactory(new MapValueFactory(TIMESTAMP_COLUMN));
      // 2. Add Value Columns and it's Data
      int idx = 0;
      while (batchData.hasCurrent()) {
        HashMap<String, SimpleStringProperty> pageInfoMap = new HashMap<>();
        // Add TimeColumn Data
        pageInfoMap.put(
            TIMESTAMP_COLUMN,
            new SimpleStringProperty(new Date(batchData.currentTime()).toString()));
        // Add Value Column and it's data
        String[] values = batchData.currentTsPrimitiveType().getStringValue().split(",");
        int measurementCounts = values.length;
        for (int i = 0; i < measurementCounts; i++) {
          // Add value columns
          if (idx == 0) {
            String measurementId =
                pageItemParams.getChunkHeaderList().get(i + 1).getMeasurementID();
            TableColumn<HashMap<String, SimpleStringProperty>, String> valueCol =
                new TableColumn<HashMap<String, SimpleStringProperty>, String>(measurementId);
            valueCol.setCellValueFactory(new MapValueFactory(VALUE_COLUMN + i));
            alignedTableView.getColumns().add(valueCol);
          }
          if (values[i] == null || values[i].length() == 0) {
            logger.error("there is a null value in a page of aligned chunk's BatchData");
            return;
          }
          values[i] = values[i].trim();
          if (i == 0) {
            values[i] = values[i].substring(1);
          } else if (i == measurementCounts - 1) {
            values[i] = values[i].substring(0, values[i].length() - 1);
          }
          pageInfoMap.put(VALUE_COLUMN + i, new SimpleStringProperty(values[i]));
        }
        columnDataList.add(pageInfoMap);
        idx++;
        batchData.next();
      }
    } catch (IOException e) {
      logger.error(
          "Failed to get Aligned Page details, the TimePage statistics:{}",
          pageInfo.getStatistics());
    }

    alignedTableView.setItems(columnDataList);
    alignedTableView.setLayoutX(0);
    alignedTableView.setLayoutY(0);
    alignedTableView.setPrefWidth(ALIGNED_PAGE_INFO_PAGE_WIDTH);
    alignedTableView.setPrefHeight(ALIGNED_PAGE_INFO_PAGE_HEIGHT);

    stage.show();
    URL uiDarkCssResource = getClass().getClassLoader().getResource("css/ui-dark.css");
    if (uiDarkCssResource != null) {
      this.getScene().getStylesheets().add(uiDarkCssResource.toExternalForm());
    }
  }
}
