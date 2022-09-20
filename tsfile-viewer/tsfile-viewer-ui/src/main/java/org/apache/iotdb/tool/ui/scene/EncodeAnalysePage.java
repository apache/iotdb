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

import org.apache.iotdb.tool.core.model.AnalysedResultModel;
import org.apache.iotdb.tool.core.model.EncodeCompressAnalysedModel;
import org.apache.iotdb.tool.ui.config.TableAlign;
import org.apache.iotdb.tool.ui.table.EncodeCompressAnalyseTable;
import org.apache.iotdb.tool.ui.view.BaseTableView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.List;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.image.ImageView;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.HBox;
import javafx.stage.Stage;

import static org.apache.iotdb.tool.ui.common.constant.StageConstant.*;

/**
 * Encode and Compress Analyse
 *
 * @author shenguanchu
 */
public class EncodeAnalysePage {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBParsePageV3.class);
  private Scene scene;
  private IoTDBParsePageV3 ioTDBParsePage;
  private ObservableList<EncodeCompressAnalyseTable> analyseDataList =
      FXCollections.observableArrayList();
  private Stage father = null;

  /** table datas */
  //  private TableView pageHeaderTableView;

  private TableView analyseTableView;

  public EncodeAnalysePage() {}

  public EncodeAnalysePage(Stage stage, IoTDBParsePageV3 ioTDBParsePage) {
    this.ioTDBParsePage = ioTDBParsePage;
    init(stage);
  }

  public Scene getScene() {
    return scene;
  }

  private void init(Stage stage) {
    //    pageHeaderTableView = new TableView();
    analyseTableView = new TableView();

    AnchorPane anchorPane = new AnchorPane();
    this.father = stage;
    scene = new Scene(anchorPane, ENCODE_ANALYSE_PAGE_WIDTH, ENCODE_ANALYSE_PAGE_HEIGHT);
    stage.setScene(scene);
    stage.setTitle("Encoding and Compressing Analysis");
    stage.show();
    stage.setResizable(false);

    // search filter
    HBox analyseBox = new HBox();
    anchorPane.getChildren().add(analyseBox);
    analyseBox.getStyleClass().add("encode-compress-analyse-box");
    analyseBox.setPrefWidth(ENCODE_ANALYSE_PAGE_WIDTH);
    analyseBox.setPrefHeight(ENCODE_ANALYSE_PAGE_HEIGHT * 0.2);

    ObservableList<Node> searchFilterBoxChildren = analyseBox.getChildren();
    Label deviceIdLabel = new Label("deviceID:");
    TextField deviceIdText = new TextField();
    Label measurementIdLabel = new Label("measurementID:");
    TextField measurementIdText = new TextField();
    Button searchButton = new Button("Analyse");
    searchButton.setGraphic(new ImageView("/icons/find-light.png"));
    searchButton.getStyleClass().add("search-button");

    searchFilterBoxChildren.addAll(
        deviceIdLabel, deviceIdText, measurementIdLabel, measurementIdText, searchButton);

    // button click event
    searchButton.setOnMouseClicked(
        event -> {
          java.lang.String deviceIdTextText = deviceIdText.getText().trim();
          java.lang.String measurementIdTextText = measurementIdText.getText().trim();
          if(deviceIdTextText == null || deviceIdTextText.trim().equals("") || measurementIdTextText == null || measurementIdTextText.trim().equals("")) {
            return;
          }
          try {
            AnalysedResultModel analysedResultModel =
                ioTDBParsePage
                    .getTsFileAnalyserV13()
                    .fetchAnalysedResultWithDeviceAndMeasurement(
                        deviceIdTextText, measurementIdTextText);
            showQueryDataSet(analysedResultModel);
          } catch (Exception exception) {
            logger.error(
                "Failed to analyse the encode and compression type of the TimeSeries, deviceId:{}, measurementId:{}",
                deviceIdTextText,
                measurementIdTextText);
          }
        });

    BaseTableView baseTableView = new BaseTableView();

    // table page data
    AnchorPane pageDataPane = new AnchorPane();
    pageDataPane.setLayoutX(0);
    pageDataPane.setLayoutY(ENCODE_ANALYSE_PAGE_HEIGHT * 0.2);
    pageDataPane.setPrefHeight(ENCODE_ANALYSE_PAGE_HEIGHT * 0.2);
    anchorPane.getChildren().add(pageDataPane);
    TableColumn<String, String> typeNameCol =
        baseTableView.genColumn(
            TableAlign.CENTER_LEFT, "typeName", "typeName", "EncodeCompressAnalyseTable");
    TableColumn<String, String> encodeNameCol =
        baseTableView.genColumn(
            TableAlign.CENTER_LEFT, "encodeName", "encodeName", "EncodeCompressAnalyseTable");
    TableColumn<String, String> compressNameCol =
        baseTableView.genColumn(
            TableAlign.CENTER_LEFT, "compressName", "compressName", "EncodeCompressAnalyseTable");
    TableColumn<String, String> originSizeCol =
        baseTableView.genColumn(
            TableAlign.CENTER_LEFT, "originSize", "originSize", "EncodeCompressAnalyseTable");
    TableColumn<String, String> encodeSizeCol =
        baseTableView.genColumn(
            TableAlign.CENTER_LEFT, "encodedSize", "encodedSize", "EncodeCompressAnalyseTable");
    TableColumn<String, String> uncompressSizeCol =
        baseTableView.genColumn(
            TableAlign.CENTER_LEFT,
            "uncompressSize",
            "uncompressSize",
            "EncodeCompressAnalyseTable");
    TableColumn<String, String> compressedSizeCol =
        baseTableView.genColumn(
            TableAlign.CENTER_LEFT,
            "compressedSize",
            "compressedSize",
            "EncodeCompressAnalyseTable");
    TableColumn<String, String> compressedRatioCol =
        baseTableView.genColumn(
            TableAlign.CENTER_LEFT,
            "compressedRatio",
            "compressedRatio",
            "EncodeCompressAnalyseTable");
    TableColumn<String, String> compressedCostCol =
        baseTableView.genColumn(
            TableAlign.CENTER_LEFT,
            "compressedCost(ns)",
            "compressedCost",
            "EncodeCompressAnalyseTable");
    TableColumn<String, String> scoreCol =
        baseTableView.genColumn(
            TableAlign.CENTER_LEFT, "score", "score", "EncodeCompressAnalyseTable");

    baseTableView.tableViewInit(
        pageDataPane,
        analyseTableView,
        analyseDataList,
        true,
        typeNameCol,
        encodeNameCol,
        compressNameCol,
        originSizeCol,
        encodeSizeCol,
        uncompressSizeCol,
        compressedSizeCol,
        compressedRatioCol,
        compressedCostCol,
        scoreCol);
    analyseTableView.setLayoutX(0);
    analyseTableView.setLayoutY(0);
    analyseTableView.setPrefWidth(ENCODE_ANALYSE_PAGE_WIDTH);
    analyseTableView.setPrefHeight(ENCODE_ANALYSE_PAGE_HEIGHT * 0.8);

    URL uiDarkCssResource = getClass().getClassLoader().getResource("css/ui-dark.css");
    if (uiDarkCssResource != null) {
      this.getScene().getStylesheets().add(uiDarkCssResource.toExternalForm());
    }
  }

  private void showQueryDataSet(AnalysedResultModel analysedResultModel) {
    analyseDataList.clear();
    EncodeCompressAnalysedModel currentAnalysed = analysedResultModel.getCurrentAnalysed();
    List<EncodeCompressAnalysedModel> analysedList = analysedResultModel.getAnalysedList();
    String compressRatioString = String.format("%.2f", currentAnalysed.getCompressedSize()/((double)currentAnalysed.getUncompressSize()));
    double compressRatio = Double.parseDouble(compressRatioString);
    // 1. currentAnalysed result
    analyseDataList.add(
        new EncodeCompressAnalyseTable(
            currentAnalysed.getTypeName(),
            currentAnalysed.getEncodeName(),
            currentAnalysed.getCompressName(),
            currentAnalysed.getOriginSize(),
            currentAnalysed.getEncodedSize(),
            currentAnalysed.getUncompressSize(),
            currentAnalysed.getCompressedSize(),
                (double) currentAnalysed.getCompressedCost(),
                compressRatio,
            currentAnalysed.getScore()));
    // 2. others analysed results
    for (EncodeCompressAnalysedModel encodeCompressAnalysedModel : analysedList) {
      if (encodeCompressAnalysedModel.getEncodeName() == currentAnalysed.getEncodeName()
          && encodeCompressAnalysedModel.getCompressName() == currentAnalysed.getCompressName()) {
        continue;
      }
      compressRatioString = String.format("%.2f", encodeCompressAnalysedModel.getCompressedSize()/((double)encodeCompressAnalysedModel.getUncompressSize()));
      compressRatio = Double.parseDouble(compressRatioString);
      analyseDataList.add(
          new EncodeCompressAnalyseTable(
              encodeCompressAnalysedModel.getTypeName(),
              encodeCompressAnalysedModel.getEncodeName(),
              encodeCompressAnalysedModel.getCompressName(),
              encodeCompressAnalysedModel.getOriginSize(),
              encodeCompressAnalysedModel.getEncodedSize(),
              encodeCompressAnalysedModel.getUncompressSize(),
              encodeCompressAnalysedModel.getCompressedSize(),
                  (double) encodeCompressAnalysedModel.getCompressedCost(),
                  compressRatio,
              encodeCompressAnalysedModel.getScore()));
    }

    analyseTableView.setVisible(true);
  }

  public void close() {
    analyseDataList.clear();
    this.father.close();
  }
}
