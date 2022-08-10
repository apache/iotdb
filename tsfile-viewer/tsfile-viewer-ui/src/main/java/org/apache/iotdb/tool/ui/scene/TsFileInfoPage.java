package org.apache.iotdb.tool.ui.scene;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleLongProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.stage.Stage;

public class TsFileInfoPage {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBParsePageV3.class);

  private static final double WIDTH = 455;
  private static final double HEIGHT = 345;

  private IoTDBParsePageV3 ioTDBParsePage;
  private GridPane pane;
  private Scene scene;
  private Stage stage;
  private TableView fileInfoTableView = new TableView();
  private ObservableList<FileInfo> fileDatas = FXCollections.observableArrayList();
  private String tsfileName;

  public TsFileInfoPage() {}

  public TsFileInfoPage(Stage stage, IoTDBParsePageV3 ioTDBParsePage, String tsfileName) {
    this.stage = stage;
    this.ioTDBParsePage = ioTDBParsePage;
    this.tsfileName = tsfileName;
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

    pane.setHgap(10);
    pane.setVgap(10);

    pane.setPadding(new Insets(20));
    pane.setAlignment(Pos.CENTER);

    Label tsfileNameLabel = new Label("TSFileName:");
    TextField tsfileNameResult = new TextField(tsfileName);
    tsfileNameResult.setEditable(false);
    tsfileNameResult.setFocusTraversable(false);
    tsfileNameResult.getStyleClass().add("copiable-text");
    pane.add(tsfileNameLabel, 0, 0);
    pane.add(tsfileNameResult, 1, 0);

    Label versionLabel = new Label("Version:");
    TextField versionResult =
        new TextField(ioTDBParsePage.getTsFileAnalyserV13().getVersion() + "");
    versionResult.setEditable(false);
    versionResult.setFocusTraversable(false);
    versionResult.getStyleClass().add("copiable-text");
    pane.add(versionLabel, 0, 1);
    pane.add(versionResult, 1, 1);

    Label sizeLabel = new Label("Size:");
    TextField sizeResult =
        new TextField(ioTDBParsePage.getTsFileAnalyserV13().getFileSize() / 1024 + "MB");
    sizeResult.setEditable(false);
    sizeResult.setFocusTraversable(false);
    sizeResult.getStyleClass().add("copiable-text");
    pane.add(sizeLabel, 0, 2);
    pane.add(sizeResult, 1, 2);

    Label dataCountsLabel = new Label("DataCounts:");
    TextField dataCountsResult =
        new TextField(ioTDBParsePage.getTsFileAnalyserV13().getAllCount() + "");
    dataCountsResult.setEditable(false);
    dataCountsResult.setFocusTraversable(false);
    dataCountsResult.getStyleClass().add("copiable-text");
    pane.add(dataCountsLabel, 0, 3);
    pane.add(dataCountsResult, 1, 3);

    stage.show();
    URL uiDarkCssResource = getClass().getClassLoader().getResource("css/copiable-text.css");
    if (uiDarkCssResource != null) {
      this.getScene().getStylesheets().add(uiDarkCssResource.toExternalForm());
    }
  }

  public static class FileInfo {

    private final SimpleIntegerProperty fileVersion;
    private final SimpleLongProperty fileSize;
    private final SimpleLongProperty dataCounts;

    public FileInfo(int fileVersion, long fileSize, long dataCounts) {
      this.fileVersion = new SimpleIntegerProperty(fileVersion);
      this.fileSize = new SimpleLongProperty(fileSize);
      this.dataCounts = new SimpleLongProperty(dataCounts);
    }

    public int getFileVersion() {
      return fileVersion.get();
    }

    public SimpleIntegerProperty fileVersionProperty() {
      return fileVersion;
    }

    public void setFileVersion(int fileVersion) {
      this.fileVersion.set(fileVersion);
    }

    public long getFileSize() {
      return fileSize.get();
    }

    public SimpleLongProperty fileSizeProperty() {
      return fileSize;
    }

    public void setFileSize(int fileSize) {
      this.fileSize.set(fileSize);
    }

    public long getDataCounts() {
      return dataCounts.get();
    }

    public SimpleLongProperty dataCountsProperty() {
      return dataCounts;
    }

    public void setDataCounts(int dataCounts) {
      this.dataCounts.set(dataCounts);
    }
  }
}
