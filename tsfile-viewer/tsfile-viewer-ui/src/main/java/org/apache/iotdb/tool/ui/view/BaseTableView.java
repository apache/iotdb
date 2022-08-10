package org.apache.iotdb.tool.ui.view;

import org.apache.iotdb.tool.ui.config.TableAlign;

import javafx.collections.ObservableList;
import javafx.scene.Node;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.Tooltip;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.Pane;
import javafx.util.Callback;
import org.apache.iotdb.tool.ui.scene.IoTDBParsePageV3;

/**
 * base table view
 *
 * @author shenguanchu
 */
public class BaseTableView {

  public BaseTableView() {}

  public void tableViewInit(
      Pane pane,
      TableView tableView,
      ObservableList datas,
      boolean isShow,
      TableColumn... genColumn) {
    tableView.setItems(datas);
    tableView.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
    pane.getChildren().add(tableView);
    tableView.getColumns().addAll(genColumn);
    tableView.setVisible(isShow);
//    tableView.getSelectionModel().selectedItemProperty().addListener((observable, oldValue, newValue) -> {
//      Object value = observable.getValue();
//      if(value != null && value instanceof IoTDBParsePageV3.TimesValues) {
//        IoTDBParsePageV3.TimesValues cur = (IoTDBParsePageV3.TimesValues) value;
//        System.out.println(cur.getTimestamp() + "," + cur.getValue());
//        System.out.println(observable);
//        System.out.println(oldValue);
////        System.out.println(tableView.getItems().get(tableView.getItems().indexOf(observable)));
//      }
//    });
  }

  public TableColumn genColumn(TableAlign align, String showName, String name, String tableName) {
    if (align == null) {
      align = TableAlign.CENTER;
    }
    TableColumn column = new TableColumn<>(showName);
    column.setCellValueFactory(new PropertyValueFactory<>(name));
    column.setCellFactory(
        new Callback<TableColumn<?, ?>, TableCell<?, ?>>() {
          private final Tooltip tooltip = new Tooltip();

          @Override
          public TableCell<?, ?> call(TableColumn<?, ?> param) {
            return new TableCell<Object, Object>() {
              @Override
              protected void updateItem(Object item, boolean empty) {
                if (item == getItem()) {
                  return;
                }
                super.updateItem(item, empty);
                if (item == null) {
                  super.setText(null);
                  super.setGraphic(null);
                } else if (item instanceof Node) {
                  super.setText(null);
                  super.setGraphic((Node) item);
                } else {
                  // tool tip
                  super.setText(item.toString());
                  super.setGraphic(null);
                  super.setTooltip(tooltip);
                  tooltip.setText(item.toString());
                  // TODO
                  if(tableName != null && "EncodeCompressAnalyseTable".equals(tableName) && getIndex() == 0) {
                    super.setStyle("-fx-background-color: #607B8B");
                  }
                }
              }
            };
          }
        });
    column.setStyle("-fx-alignment: " + align.getAlign() + ";");
    return column;
  }
}
