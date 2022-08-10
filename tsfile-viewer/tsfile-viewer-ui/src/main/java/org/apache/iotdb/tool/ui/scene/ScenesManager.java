package org.apache.iotdb.tool.ui.scene;

import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.event.EventHandler;
import javafx.scene.control.Alert;
import javafx.scene.control.ButtonType;
import javafx.scene.control.ProgressBar;
import javafx.stage.Stage;
import javafx.stage.WindowEvent;
import org.apache.iotdb.tool.core.util.OffLineTsFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * scenes manager for scene change
 *
 * @author oortCloudFei
 */
public class ScenesManager {

  private static final Logger logger = LoggerFactory.getLogger(ScenesManager.class);

  static final ScenesManager scenesManager = new ScenesManager();

  private Page curPage = null;

  private IoTDBParsePageV13 ioTDBParsePage;

  /** base stage */
  private Stage baseStage = null;

  private Map<SceneType, Page> pages = new HashMap<>(8);

  private ScenesManager() {}

  public IoTDBParsePageV13 getIoTDBParsePage() {
    return ioTDBParsePage;
  }

  public static final ScenesManager getInstance() {
    return scenesManager;
  }

  public void setBaseStage(Stage stage) {
    this.baseStage = stage;
  }

  public void converToIoTDBParsePage(Stage stage) {
    converToPage(stage, SceneType.TSFILE_PARSE_PAGE);
  }

  /**
   * conver to page for page inside
   *
   * @param sceneType
   */
  public void converToPage(SceneType sceneType) {
    if (this.baseStage != null) {
      converToPage(this.baseStage, sceneType);
    }
  }

  public void loadTsFile(String filePath, ProgressBar progressBar) {
    if (!fileCheck(filePath)) {
      return;
    }
    ioTDBParsePage = new IoTDBParsePageV13(filePath);

    Task progressTask = progressWorker(ioTDBParsePage, progressBar);
    progressBar.progressProperty().unbind();
    progressBar.progressProperty().bind(progressTask.progressProperty());
    new Thread(progressTask).start();
  }

  public void showBaseStage() {
    ioTDBParsePage.init();
    baseStage.setScene(ioTDBParsePage.getScene());
    baseStage.setTitle(ioTDBParsePage.getName());
    baseStage.centerOnScreen();
    baseStage.show();
  }

  public void converToPage(Stage stage, SceneType sceneType) {
    if (stage == null) {
      logger.error("Stage is null");
      return;
    }
    Page page;
    switch (sceneType) {
      case HOME:
        page = pages.computeIfAbsent(SceneType.HOME, (key) -> new HomePage());
        break;
      case TSFILE_CHOOSE:
        page =
            pages.computeIfAbsent(SceneType.TSFILE_CHOOSE, (key) -> new TsFileChooserPage(stage));
        logger.info("TsFile Choose, the page: {}", page);
        break;
      default:
        logger.info("Unexpect sceneType, the sceneType: {}", sceneType);
        return;
    }

    stage.setOnCloseRequest(new EventHandler<WindowEvent>(){
      public void handle(WindowEvent event) {
        Platform.exit();
      }
    });

    stage.setScene(page.getScene());
    stage.setTitle(page.getName());
    stage.show();
    curPage = page;
  }

  public boolean fileCheck(String path) {
    int i = 0;
    try {
      i = OffLineTsFileUtil.fetchTsFileVersionNumber(path);
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (i != 3) {
      Alert alert = new Alert(Alert.AlertType.ERROR, "none support version", ButtonType.OK);
      alert.showAndWait();
      return false;
    }
    return true;
  }

  public Task progressWorker(IoTDBParsePageV13 ioTDBParsePage, ProgressBar progressBar) {
    return new Task() {
      @Override
      protected Object call() throws Exception {
        while (ioTDBParsePage.getTsFileAnalyserV13().getRateOfProcess() < 1) {
          updateProgress(ioTDBParsePage.getTsFileAnalyserV13().getRateOfProcess(), 1);
        }
        updateProgress(1, 1);
        logger.info("TsFile Load completed.");
        TsFileChooserPage.isFileLoaded = true;
        Platform.runLater(()->showBaseStage());
        return true;
      }
    };
  }

  public enum SceneType {
    HOME,
    TSFILE_CHOOSE,
    TSFILE_PARSE_PAGE
  }
}
