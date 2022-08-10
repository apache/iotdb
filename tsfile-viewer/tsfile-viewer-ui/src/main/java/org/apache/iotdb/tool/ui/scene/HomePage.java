package org.apache.iotdb.tool.ui.scene;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.layout.GridPane;

import java.net.URL;

/**
 * Home Page
 *
 * @author oortCloudFei
 */
public class HomePage extends Page<GridPane> {

  private static final double WIDTH = 200;
  private static final double HEIGHT = 250;

  public HomePage() {
    super(new GridPane(), WIDTH, HEIGHT);
    init();
  }

  private void init() {

    this.root.setAlignment(Pos.CENTER);
    this.root.setHgap(10);
    this.root.setVgap(10);
    this.root.setPadding(new Insets(25, 25, 25, 25));

    Button parseButton = new Button("TSFILE解析工具");
    parseButton.setOnAction(
        e -> {
          ScenesManager.getInstance().converToPage(ScenesManager.SceneType.TSFILE_CHOOSE);
        });
    parseButton.setPrefWidth(150);
    this.root.add(parseButton, 0, 1);
    Button transButton = new Button("模拟转化工具");
    transButton.setPrefWidth(150);
    this.root.add(transButton, 0, 2);

    URL cssResource = getClass().getClassLoader().getResource("css/IoTDB.css");
    if (cssResource != null) {
      this.getScene().getStylesheets().add(cssResource.toExternalForm());
    }
  }

  @Override
  public String getName() {
    return "IoTDB-TSFILE工具";
  }
}
