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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import javafx.scene.Scene;
import javafx.scene.control.TextArea;
import javafx.scene.layout.AnchorPane;
import javafx.stage.Stage;

public class IndexNodeInfoPage {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBParsePageV3.class);

  private static final double WIDTH = 810;
  private static final double HEIGHT = 300;

  private AnchorPane anchorPane;
  private Scene scene;
  private Stage stage;

  private String menuItemInfo;

  private String nodeInfo;

  public IndexNodeInfoPage() {}

  public IndexNodeInfoPage(Stage stage, String menuItemInfo, String nodeInfo) {
    this.menuItemInfo = menuItemInfo;
    this.nodeInfo = nodeInfo;
    this.stage = stage;
    init(stage);
  }

  public Scene getScene() {
    return scene;
  }

  private void init(Stage stage) {
    anchorPane = new AnchorPane();
    scene = new Scene(anchorPane, WIDTH, HEIGHT);
    stage.setScene(scene);
    stage.setTitle(menuItemInfo);
    stage.show();
    stage.setResizable(false);

    TextArea textArea = new TextArea(nodeInfo);
    textArea.setEditable(false);
    textArea.setPrefWidth(WIDTH);
    textArea.setPrefHeight(HEIGHT);
    textArea.setWrapText(true);
    anchorPane.getChildren().add(textArea);

    URL uiDarkCssResource = getClass().getClassLoader().getResource("css/ui-dark.css");
    if (uiDarkCssResource != null) {
      this.getScene().getStylesheets().add(uiDarkCssResource.toExternalForm());
    }
  }
}
