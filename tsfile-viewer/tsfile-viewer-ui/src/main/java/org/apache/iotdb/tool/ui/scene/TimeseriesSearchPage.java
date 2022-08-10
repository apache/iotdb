package org.apache.iotdb.tool.ui.scene;

import javafx.geometry.Orientation;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.StackPane;
import javafx.stage.Stage;

/**
 * TimeseriesSearchPage
 *
 * @author shenguanchu
 */
public class TimeseriesSearchPage {

  private static final double WIDTH = 300;
  private static final double HEIGHT = 60;

  private TextField searchTextFiel;
  private Button tsSearchButton;
  private StackPane pane;
  private Scene scene;
  private IoTDBParsePageV3 ioTDBParsePage;

  public TimeseriesSearchPage(Stage stage, IoTDBParsePageV3 ioTDBParsePage) {
    this.ioTDBParsePage = ioTDBParsePage;
    init(stage);
  }

  private void init(Stage stage) {
    pane = new StackPane();
    scene = new Scene(this.pane, WIDTH, HEIGHT);
    stage.setScene(scene);

    searchTextFiel = new TextField();
    stage.setTitle("Find by Keyword");
    searchTextFiel.setMaxHeight(200);

    tsSearchButton = new Button("find");

    FlowPane flowPane = new FlowPane();
    flowPane.setHgap(5);
    flowPane.setOrientation(Orientation.HORIZONTAL);
    flowPane.setAlignment(Pos.CENTER);
    flowPane.getChildren().addAll(searchTextFiel, tsSearchButton);

    pane.getChildren().addAll(flowPane);

    // shortcut key binding: ENTER
    KeyCombination kccb = new KeyCodeCombination(KeyCode.ENTER);
    scene.getAccelerators().put(kccb, () -> tsSearchButton.fire());

    tsSearchButton.setOnAction(
        event -> {
          String searchResult = ioTDBParsePage.timeseriesSearch(searchTextFiel.getText());

          if (searchResult == null) {
            return;
          }
          // find by path
          ioTDBParsePage.chooseTree(searchResult);
        });

    stage.show();
  }
}
