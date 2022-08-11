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

import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.scene.control.*;
import javafx.scene.image.Image;
import javafx.stage.Stage;

/**
 * scenes manager for scene change
 *
 * @author oortCloudFei
 */
public class ScenesManager {

  private static final Logger logger = LoggerFactory.getLogger(ScenesManager.class);

  static final ScenesManager scenesManager = new ScenesManager();

  private IoTDBParsePageV3 ioTDBParsePage;

  /** base stage */
  private Stage baseStage = null;

  private ScenesManager() {
    ioTDBParsePage = new IoTDBParsePageV3();
  }

  public IoTDBParsePageV3 getIoTDBParsePage() {
    return ioTDBParsePage;
  }

  public static final ScenesManager getInstance() {
    return scenesManager;
  }

  public void setBaseStage(Stage stage) {
    this.baseStage = stage;
  }

  public void loadTsFile(ProgressBar progressBar) {
    Task progressTask = progressWorker(ioTDBParsePage);
    progressBar.progressProperty().unbind();
    progressBar.progressProperty().bind(progressTask.progressProperty());
    new Thread(progressTask).start();
  }

  public void showBaseStage() {
    ioTDBParsePage.init(baseStage);
    baseStage.setScene(ioTDBParsePage.getScene());
    baseStage.setTitle(ioTDBParsePage.getName());
    baseStage.getIcons().add(new Image("/icons/yonyou-logo.png"));
    baseStage.centerOnScreen();
    baseStage.show();
    // 关闭 stage 时清空缓存
    baseStage.setOnCloseRequest(
        event -> {
          clearCache();
          baseStage = null;
        });
  }

  public Task progressWorker(IoTDBParsePageV3 ioTDBParsePage) {
    return new Task() {
      @Override
      protected Object call() throws Exception {
        long loadFileStartTime = System.currentTimeMillis();
        while (ioTDBParsePage.getTsFileAnalyserV13().getRateOfProcess() < 1) {
          updateProgress(ioTDBParsePage.getTsFileAnalyserV13().getRateOfProcess(), 1);
        }
        updateProgress(1, 1);
        logger.info("TsFile Load completed.");
        System.out.println("TsFile Load completed.");
        long loadFileEndTime = System.currentTimeMillis();
        System.out.println("load file total time cost: " + (loadFileEndTime - loadFileStartTime));
        Platform.runLater(() -> ioTDBParsePage.chunkGroupTreeDataInit());
        return true;
      }
    };
  }

  // 清空缓存
  public void clearCache() {
    ioTDBParsePage.clearParsePageCache();
  }
}
