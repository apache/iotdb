package org.apache.iotdb.tool.ui;

import org.apache.iotdb.tool.ui.scene.ScenesManager;

import javafx.application.Application;
import javafx.stage.Stage;

/**
 * IoTDB UI Application
 *
 * <p>query、binary、group chunk、index
 *
 * @author oortCloudFei
 */
public class IUIApplication extends Application {

  @Override
  public void start(Stage primaryStage) throws Exception {

    ScenesManager manager = ScenesManager.getInstance();
    // put base stage into scene manager
    manager.setBaseStage(primaryStage);
    primaryStage.setResizable(false);
    // application start
    manager.showBaseStage();
  }
}
