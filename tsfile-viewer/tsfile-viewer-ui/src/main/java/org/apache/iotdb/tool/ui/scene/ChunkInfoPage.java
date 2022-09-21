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

import org.apache.iotdb.tsfile.file.header.ChunkHeader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.TreeItem;
import javafx.scene.layout.GridPane;
import javafx.stage.Stage;

public class ChunkInfoPage {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBParsePageV3.class);

  private static final double WIDTH = 455;
  private static final double HEIGHT = 345;

  private IoTDBParsePageV3 ioTDBParsePage;
  private GridPane pane;
  private Scene scene;
  private Stage stage;
  // TODO
  private TableView fileInfoTableView = new TableView();
  private ObservableList<TsFileInfoPage.FileInfo> fileDatas = FXCollections.observableArrayList();

  private TreeItem<IoTDBParsePageV3.ChunkTreeItemValue> chunkItem;

  public ChunkInfoPage() {}

  public ChunkInfoPage(
      Stage stage,
      IoTDBParsePageV3 ioTDBParsePage,
      TreeItem<IoTDBParsePageV3.ChunkTreeItemValue> chunkItem) {
    this.stage = stage;
    this.ioTDBParsePage = ioTDBParsePage;
    this.chunkItem = chunkItem;
    init(stage);
  }

  public Scene getScene() {
    return scene;
  }

  private void init(Stage stage) {
    stage.setResizable(false);
    pane = new GridPane();
    scene = new Scene(this.pane, WIDTH, HEIGHT);
    stage.setScene(scene);
    stage.setTitle("Chunk Information");

    pane.setHgap(10);
    pane.setVgap(10);

    pane.setPadding(new Insets(20));
    pane.setAlignment(Pos.CENTER);

    // 数据来源
    IoTDBParsePageV3.ChunkWrap params =
        (IoTDBParsePageV3.ChunkWrap) chunkItem.getValue().getParams();

    ChunkHeader chunkHeader = params.getChunkHeader();

    Label dataSizeLabel = new Label("DataSize(b):");
    TextField dataSizeResult = new TextField(chunkHeader.getDataSize() + "");
    dataSizeResult.setEditable(false);
    dataSizeResult.setFocusTraversable(false);
    dataSizeResult.getStyleClass().add("copiable-text");
    pane.add(dataSizeLabel, 0, 0);
    pane.add(dataSizeResult, 1, 0);

    Label dataTypeLabel = new Label("DataType:");
    TextField dataTypeResult = new TextField(chunkHeader.getDataType() + "");
    dataTypeResult.setEditable(false);
    dataTypeResult.setFocusTraversable(false);
    dataTypeResult.getStyleClass().add("copiable-text");
    pane.add(dataTypeLabel, 0, 1);
    pane.add(dataTypeResult, 1, 1);

    Label compressionLabel = new Label("CompressionType:");
    TextField compressionResult = new TextField(chunkHeader.getCompressionType() + "");
    compressionResult.setEditable(false);
    compressionResult.setFocusTraversable(false);
    compressionResult.getStyleClass().add("copiable-text");
    pane.add(compressionLabel, 0, 2);
    pane.add(compressionResult, 1, 2);

    Label encodingTypeLabel = new Label("EncodingType:");
    TextField encodingTypeResult = new TextField(chunkHeader.getEncodingType() + "");
    encodingTypeResult.setEditable(false);
    encodingTypeResult.setFocusTraversable(false);
    encodingTypeResult.getStyleClass().add("copiable-text");
    pane.add(encodingTypeLabel, 0, 3);
    pane.add(encodingTypeResult, 1, 3);

    stage.show();
    URL uiDarkCssResource = getClass().getClassLoader().getResource("css/copiable-text.css");
    if (uiDarkCssResource != null) {
      this.getScene().getStylesheets().add(uiDarkCssResource.toExternalForm());
    }
  }
}
