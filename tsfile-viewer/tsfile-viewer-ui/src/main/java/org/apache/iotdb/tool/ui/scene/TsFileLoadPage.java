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

import org.apache.iotdb.tool.core.service.TsFileAnalyserV13;
import org.apache.iotdb.tool.core.util.OffLineTsFileUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.geometry.Rectangle2D;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressBar;
import javafx.scene.layout.GridPane;
import javafx.stage.DirectoryChooser;
import javafx.stage.Screen;
import javafx.stage.Stage;

/**
 * TsFileLoadPage
 *
 * @author shenguanchu
 */
public class TsFileLoadPage {
  private static final Logger logger = LoggerFactory.getLogger(ScenesManager.class);

  private static final double WIDTH = 540;
  private static final double HEIGHT = 200;

  private Button loadButton;
  private Button cancelButton;
  private GridPane pane;
  private Scene scene;
  private Stage stage;

  private ProgressBar progressBar = new ProgressBar(0);

  private TsFileAnalyserV13 tsFileAnalyserV13;

  private IoTDBParsePageV3 ioTDBParsePage;

  private String filePath;

  public TsFileLoadPage() {}

  public TsFileLoadPage(Stage stage, String filePath) {
    this.stage = stage;
    this.filePath = filePath;
    init(stage);
  }

  public Scene getScene() {
    return scene;
  }

  public void setIoTDBParsePageV13(IoTDBParsePageV3 ioTDBParsePage) {
    this.ioTDBParsePage = ioTDBParsePage;
  }

  private void init(Stage stage) {
    stage.setResizable(false);
    pane = new GridPane();
    scene = new Scene(this.pane, WIDTH, HEIGHT);
    stage.setScene(scene);

    pane.setHgap(10);
    pane.setVgap(10);
    pane.setPadding(new Insets(20));
    pane.setAlignment(Pos.CENTER);
    pane.getStyleClass().add("tsfile-load-pane");

    String[] filePathArr = filePath.split("\\\\");
    String tsfileName = filePathArr[filePathArr.length - 1];
    Label infoLabel = new Label("Please confirm whether to load:");
    pane.add(infoLabel, 0, 0);
    Label tsfileNameLabel = new Label(tsfileName);
    pane.add(tsfileNameLabel, 0, 1);

    stage.setTitle("Confirm Loading");
    loadButton = new Button("open");
    cancelButton = new Button("cancel");
    pane.add(loadButton, 1, 2);
    pane.add(cancelButton, 2, 2);
    loadButton.setMinWidth(50);
    cancelButton.setMinWidth(60);

    pane.add(progressBar, 0, 2);
    progressBar.getStyleClass().add("progress-bar-field");
    progressBar.setPrefWidth(200);
    progressBar.setVisible(false);
    progressBar.setDisable(true);

    cancelButton.setOnAction(
        event -> {
          stage.close();
        });

    loadButton.setOnAction(
        event -> {
          // 1. 将 cancelButton 设为不可用
          cancelButton.setDisable(true);
          loadButton.setDisable(true);

          // 2. 清空上一个 tsfile 相关缓存（若不是第一次加载）
          if (ioTDBParsePage.getLoadedTSFileName() != null) {
            ScenesManager.getInstance().clearCache();
          }

          // 3. 异步加载文件
          loadTsFile(filePath);
          ioTDBParsePage.setTsFileAnalyserV13(tsFileAnalyserV13);
          // 进度条
          ScenesManager scenesManager = ScenesManager.getInstance();
          progressBar.setVisible(true);
          progressBar.setDisable(false);
          scenesManager.loadTsFile(progressBar);
        });

    stage.show();

    // The stage is centered on the screen
    Rectangle2D screenBounds = Screen.getPrimary().getVisualBounds();
    stage.setX((screenBounds.getWidth() - 400) / 2);
    stage.setY((screenBounds.getHeight() - 400) / 2);

    URL uiDarkCssResource = getClass().getClassLoader().getResource("css/ui-dark.css");
    if (uiDarkCssResource != null) {
      this.getScene().getStylesheets().add(uiDarkCssResource.toExternalForm());
    }
  }

  public File loadFolder(Stage baseStage) {
    DirectoryChooser directoryChooser = new DirectoryChooser();
    directoryChooser.setTitle("Open Folder");

    // directoryCache.txt
    File cacheFile = new File("directoryCache.txt");
    if (cacheFile.exists()) {
      try (InputStream inputStream = new FileInputStream(cacheFile)) {
        byte[] bytes = new byte[(int) cacheFile.length()];
        // Read the contents of the directoryCache.txt
        inputStream.read(bytes);
        File directory = new File(new String(bytes));
        if (directory.exists()) {
          directoryChooser.setInitialDirectory(directory);
        }
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e1) {
        e1.printStackTrace();
      }
    }

    File selectedfolder = directoryChooser.showDialog(baseStage);

    // Store the directory to the directoryCache.txt
    if (selectedfolder != null) {
      try (OutputStream outputStream = new FileOutputStream(cacheFile)) {
        byte[] bytes = selectedfolder.getAbsolutePath().getBytes();
        outputStream.write(bytes);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return selectedfolder;
  }

  public Boolean fileTypeCheck(String filePath) {
    return filePath.endsWith(".tsfile");
  }

  public boolean fileVersionCheck(String filePath) {
    int version = 0;
    try {
      version = OffLineTsFileUtil.fetchTsFileVersionNumber(filePath);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return version == 3;
  }

  private void loadTsFile(String filePath) {
    try {
      // tsfile parse
      this.tsFileAnalyserV13 = new TsFileAnalyserV13(filePath);
    } catch (IOException e) {
      logger.error("Failed to get TsFileAnalysedV13 instance.");
      e.printStackTrace();
      return;
    }
  }
}
