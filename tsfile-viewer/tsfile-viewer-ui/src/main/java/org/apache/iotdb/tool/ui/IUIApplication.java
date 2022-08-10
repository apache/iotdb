package org.apache.iotdb.tool.ui;

import javafx.application.Application;
import javafx.stage.Stage;
import org.apache.iotdb.tool.ui.scene.ScenesManager;

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
    manager.converToPage(primaryStage, ScenesManager.SceneType.HOME);
  }
}
