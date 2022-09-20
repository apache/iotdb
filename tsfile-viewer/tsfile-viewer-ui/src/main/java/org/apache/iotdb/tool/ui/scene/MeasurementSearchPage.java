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

import org.apache.iotdb.tool.ui.config.TableAlign;
import org.apache.iotdb.tool.ui.view.BaseTableView;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import com.browniebytes.javafx.control.DateTimePicker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.time.ZoneId;
import java.util.Date;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.image.ImageView;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

import static org.apache.iotdb.tool.ui.common.constant.StageConstant.*;

/**
 * measurement search stage
 *
 * @author shenguanchu
 */
public class MeasurementSearchPage {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBParsePageV3.class);

  private AnchorPane anchorPane;
  private Scene scene;
  private IoTDBParsePageV3 ioTDBParsePage;

  private VBox searchFilterBox;

  private AnchorPane searchResultPane;

  /** table datas */
  private TableView<String> tvTableView;

  private Stage parent = null;

  private ObservableList<IoTDBParsePageV3.TimesValues> tvDatas =
      FXCollections.observableArrayList();

  public MeasurementSearchPage(Stage stage, IoTDBParsePageV3 ioTDBParsePage) {
    this.ioTDBParsePage = ioTDBParsePage;
    init(stage);
  }

  public Scene getScene() {
    return scene;
  }

  private void init(Stage stage) {

    this.parent = stage;
    anchorPane = new AnchorPane();
    scene = new Scene(anchorPane, MEASUREMENT_SEARCH_PAGE_WIDTH, MEASUREMENT_SEARCH_PAGE_HEIGHT);
    stage.setScene(scene);
    stage.setTitle("Search: Measurement");
    stage.show();

    // search filter
    searchFilterBox = new VBox();
    anchorPane.getChildren().add(searchFilterBox);
    searchFilterBox.getStyleClass().add("search-filter-box");
    searchFilterBox.setPrefWidth(SEARCH_FILTER_BOX_WIDTH);
    searchFilterBox.setPrefHeight(SEARCH_FILTER_BOX_HEIGHT);
    stage
        .heightProperty()
        .addListener(
            (observable, oldValue, newValue) -> {
              searchFilterBox.setPrefHeight(stage.getHeight());
            });
    stage
        .widthProperty()
        .addListener(
            (observable, oldValueb, newValue) -> {
              searchFilterBox.setPrefWidth((int) stage.getWidth() >> 2);
            });

    Label startTime = new Label("startTime:");
    DateTimePicker startPicker = new DateTimePicker();
    Label endTime = new Label("endTime:");
    DateTimePicker endPicker = new DateTimePicker();
    ObservableList<Node> searchFilterBoxChildren = searchFilterBox.getChildren();
    searchFilterBoxChildren.addAll(startTime, startPicker, endTime, endPicker);

    Label deviceIdLabel = new Label("deviceID:");
    TextField deviceIdText = new TextField();
    Label measurementIdLabel = new Label("measurementID:");
    TextField measurementIdText = new TextField();
    Button searchButton = new Button("Search");
    searchButton.setGraphic(new ImageView("/icons/find-light.png"));
    searchButton.getStyleClass().add("search-button");

    searchFilterBoxChildren.addAll(
        deviceIdLabel, deviceIdText, measurementIdLabel, measurementIdText, searchButton);

    // button click event
    searchButton.setOnMouseClicked(
        event -> {
          // TODO 换成工具类
          long startLocalTime =
              startPicker
                  .dateTimeProperty()
                  .getValue()
                  .atZone(ZoneId.systemDefault())
                  .toInstant()
                  .toEpochMilli();
          long endLocalTime =
              endPicker
                  .dateTimeProperty()
                  .getValue()
                  .atZone(ZoneId.systemDefault())
                  .toInstant()
                  .toEpochMilli();
          String deviceIdTextText = deviceIdText.getText().trim();
          String measurementIdTextText = measurementIdText.getText().trim();
          try {
            QueryDataSet queryDataSet =
                ioTDBParsePage
                    .getTsFileAnalyserV13()
                    .queryResult(
                        startLocalTime,
                        endLocalTime,
                        deviceIdTextText,
                        measurementIdTextText,
                        "",
                        0,
                        0);
            showQueryDataSet(queryDataSet);
          } catch (Exception exception) {
            logger.error(
                "Failed to query data set, deviceId:{}, measurementId:{}",
                deviceIdTextText,
                measurementIdTextText);
          }
        });

    // search result
    searchResultPane = new AnchorPane();
    searchResultPane.setLayoutX(stage.getWidth() / 4);
    searchResultPane.setLayoutY(0);
    searchResultPane.setPrefHeight(stage.getHeight());
    searchResultPane.setPrefWidth(3 * stage.getWidth() / 4);
    anchorPane.getChildren().add(searchResultPane);
    stage
        .heightProperty()
        .addListener(
            (observable, oldValue, newValue) -> {
              searchResultPane.setPrefHeight(stage.getHeight());
            });
    stage
        .widthProperty()
        .addListener(
            (observable, oldValue, newValue) -> {
              searchResultPane.setLayoutX(searchFilterBox.getWidth());
              searchResultPane.setPrefWidth(stage.getWidth() - searchFilterBox.getWidth());
            });

    // TVTable 初始化与写入数据
    tvTableView = new TableView<String>();
    BaseTableView baseTableView = new BaseTableView();
    TableColumn<Date, String> timestampCol =
        baseTableView.genColumn(TableAlign.CENTER, "timestamp", "timestamp", null);
    TableColumn<String, String> valueCol =
        baseTableView.genColumn(TableAlign.CENTER_LEFT, "value", "value", null);
    baseTableView.tableViewInit(
        searchResultPane, tvTableView, tvDatas, true, timestampCol, valueCol);
    tvTableView.setLayoutX(searchFilterBox.getWidth());
    tvTableView.setLayoutY(0);
    tvTableView.setPrefWidth(searchResultPane.getPrefWidth() - 15);
    tvTableView.setPrefHeight(searchResultPane.getPrefHeight());

    searchResultPane
        .widthProperty()
        .addListener(
            (observable, oldValue, newValue) -> {
              tvTableView.setPrefWidth(searchResultPane.getPrefWidth() - 15);
            });
    searchResultPane
        .heightProperty()
        .addListener(
            (observable, oldValue, newValue) -> {
              tvTableView.setPrefHeight(searchResultPane.getPrefHeight());
            });

    URL uiDarkCssResource = getClass().getClassLoader().getResource("css/ui-dark.css");
    if (uiDarkCssResource != null) {
      this.getScene().getStylesheets().add(uiDarkCssResource.toExternalForm());
    }
  }

  public void showQueryDataSet(QueryDataSet queryDataSet) throws Exception {
    tvDatas.clear();
    while (queryDataSet.hasNext()) {
      RowRecord next = queryDataSet.next();
      StringBuilder sb = new StringBuilder();
      for (Field f : next.getFields()) {
        sb.append("\t");
        sb.append(f);
      }
      // TODO LocalDateTime
      tvDatas.add(
          new IoTDBParsePageV3.TimesValues(
              new Date(next.getTimestamp()).toString(), sb.toString()));
    }
    tvTableView.setVisible(true);
  }

  public void show() {
    if (this.parent != null && !this.parent.isShowing()) {
      this.parent.show();
    }
  }

  public void close() {
    if (this.parent != null) {
      this.tvDatas.clear();
      this.parent.close();
    }
  }

  public boolean isShow() {
    return this.parent == null ? false : this.parent.isShowing();
  }
}
