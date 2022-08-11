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
import org.apache.iotdb.tool.core.model.PageInfo;
import org.apache.iotdb.tool.ui.config.TableAlign;
import org.apache.iotdb.tool.ui.view.BaseTableView;
import org.apache.iotdb.tsfile.read.common.BatchData;

import com.sun.org.apache.xpath.internal.operations.String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Date;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.AnchorPane;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.StageStyle;

public class PageInfoPage {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBParsePageV3.class);

  private static final double WIDTH = 810;
  private static final double HEIGHT = 300;

  private Scene scene;
  private IoTDBParsePageV3 ioTDBParsePage;

  private TreeItem<IoTDBParsePageV3.ChunkTreeItemValue> pageItem;

  private ObservableList<IoTDBParsePageV3.TimesValues> tvDatas =
      FXCollections.observableArrayList();

  /** table datas */
  private TableView pageHeaderTableView;

  private TableView pageTVTableView;

  public PageInfoPage() {}

  public PageInfoPage(
      Stage stage,
      IoTDBParsePageV3 ioTDBParsePage,
      TreeItem<IoTDBParsePageV3.ChunkTreeItemValue> pageItem) {
    this.ioTDBParsePage = ioTDBParsePage;
    this.pageItem = pageItem;
    init(stage);
  }

  public Scene getScene() {
    return scene;
  }

  private void init(Stage stage) {
    // page of Aligned ChunkGroup
    if (!(pageItem.getValue().getParams() instanceof PageInfo)) {
      Stage chunkInfoStage = new Stage();
      chunkInfoStage.initStyle(StageStyle.UTILITY);
      chunkInfoStage.initModality(Modality.APPLICATION_MODAL);
      AlignedPageInfoPage alignedPageInfoPage =
          new AlignedPageInfoPage(stage, ioTDBParsePage, pageItem);
      return;
    }

    pageHeaderTableView = new TableView();
    pageTVTableView = new TableView();

    AnchorPane anchorPane = new AnchorPane();
    scene = new Scene(anchorPane, WIDTH, HEIGHT);
    stage.setScene(scene);
    stage.setTitle("Page Information");
    stage.show();
    stage.setResizable(false);

    IPageInfo pageInfo = (PageInfo) pageItem.getValue().getParams();
    AnchorPane pageHeaderPane = new AnchorPane();
    pageHeaderPane.setLayoutX(0);
    pageHeaderPane.setLayoutY(0);
    pageHeaderPane.setPrefHeight(WIDTH);
    pageHeaderPane.setPrefWidth(HEIGHT * 0.3);
    anchorPane.getChildren().add(pageHeaderPane);

    StringBuilder sb = new StringBuilder();
    sb.append("uncompressedSize: ").append(pageInfo.getUncompressedSize()).append("\n");
    sb.append("compressedSize: ").append(pageInfo.getCompressedSize()).append("\n");
    sb.append("statistics: ");
    if (pageInfo.getStatistics() == null) {
      sb.append("null");
    } else {
      sb.append(pageInfo.getStatistics().toString());
    }

    TextArea pageHeaderInfo = new TextArea(sb.toString());
    pageHeaderInfo.setEditable(false);
    pageHeaderInfo.setPrefWidth(WIDTH);
    pageHeaderInfo.setWrapText(true);
    pageHeaderPane.getChildren().add(pageHeaderInfo);

    // 数据来源
    try {
      BatchData batchData =
          ioTDBParsePage
              .getTsFileAnalyserV13()
              .fetchBatchDataByPageInfo((PageInfo) pageItem.getValue().getParams());
      while (batchData.hasCurrent()) {
        Object currValue = batchData.currentValue();
        this.tvDatas.add(
            new IoTDBParsePageV3.TimesValues(
                new Date(batchData.currentTime()).toString(),
                currValue == null ? "" : currValue.toString()));
        batchData.next();
      }
    } catch (Exception e) {
      logger.error(
          "Failed to get page details, the page statistics:{}",
          pageInfo.getStatistics().toString());
    }

    BaseTableView baseTableView = new BaseTableView();

    // table page data
    AnchorPane pageDataPane = new AnchorPane();
    pageDataPane.setLayoutX(0);
    pageDataPane.setLayoutY(HEIGHT * 0.15);
    pageDataPane.setPrefHeight(WIDTH);
    pageHeaderPane.setPrefWidth(HEIGHT * 0.7);
    anchorPane.getChildren().add(pageDataPane);
    TableColumn<Date, String> timestampCol =
        baseTableView.genColumn(TableAlign.CENTER, "timestamp", "timestamp", null);
    TableColumn<String, String> valueCol =
        baseTableView.genColumn(TableAlign.CENTER_LEFT, "value", "value", null);
    baseTableView.tableViewInit(
        pageDataPane, pageTVTableView, tvDatas, true, timestampCol, valueCol);
    pageTVTableView.setLayoutX(0);
    pageTVTableView.setLayoutY(HEIGHT * 0.12);
    pageTVTableView.setPrefWidth(WIDTH);
    pageTVTableView.setPrefHeight(HEIGHT * 0.65);

    URL uiDarkCssResource = getClass().getClassLoader().getResource("css/ui-dark.css");
    if (uiDarkCssResource != null) {
      this.getScene().getStylesheets().add(uiDarkCssResource.toExternalForm());
    }
  }
}
