package org.apache.iotdb.tool.ui.scene;

import com.browniebytes.javafx.control.DateTimePicker;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleLongProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.Group;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import javafx.scene.layout.GridPane;
import javafx.scene.shape.Line;
import javafx.stage.Stage;
import javafx.util.Callback;
import org.apache.iotdb.tool.core.model.ChunkGroupMetadataModel;
import org.apache.iotdb.tool.core.model.TimeSeriesMetadataNode;
import org.apache.iotdb.tool.core.service.TsFileAnalyserV13;
import org.apache.iotdb.tool.ui.node.IndexNode;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * IoTDBParsePage
 *
 * @author oortCloudFei
 */
public class IoTDBParsePageV13 extends IoTDBParsePage {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBParsePageV13.class);
  public static final double WIDTH = 1080;
  public static final double HEIGHT = 750;

  private static final String TREE_ITEM_TYPE_CHUNK_GROUP = "cg";
  private static final String TREE_ITEM_TYPE_CHUNK = "c";
  private static final String TREE_ITEM_TYPE_CHUNK_PAGE = "cp";
  private static final String TREE_ITEM_TYPE_ROOT = "r";

  /** version 13 interface */
  private TsFileAnalyserV13 tsFileAnalyserV13;

  /** index region */
  //    private ScrollRegion indexRegion;
  private Group indexGroup;

  /** Chunk Group Tree */
  private TreeItem rootItem;

  private TreeView<ChunkTreeItemValue> tree;

  /** table datas */
  private TableView tvTableView = new TableView();

  private ObservableList<TimesValues> tvDatas = FXCollections.observableArrayList();
  private TableView chunkTableView = new TableView();
  private ObservableList<ChunkInfo> chunkDatas = FXCollections.observableArrayList();
  private TableView pageTableView = new TableView();
  private ObservableList<PageInfo> pageDatas = FXCollections.observableArrayList();
  private TableView fileTableView = new TableView();

  /** click index to tree */
  private Map<String, TreeItem<ChunkTreeItemValue>> indexMap = new HashMap<>(256);

  private List<String> timeseriesList = new ArrayList<>();

  /** timeseriesSearch stage */
  private TimeseriesSearchPage timeseriesSearchPage;

  public IoTDBParsePageV13(String filePath) {
    super(new Group(), WIDTH, HEIGHT);
    // 异步
    try {
      // tsfile parse
      this.tsFileAnalyserV13 = new TsFileAnalyserV13(filePath);
    } catch (IOException e) {
      logger.error("Failed to get TsFileAnalysedV13 instance.");
      e.printStackTrace();
      return;
    }
  }

  // public
  public void init() {
    // 等待文件异步加载完成
    while (true) {
      if (tsFileAnalyserV13.getRateOfProcess() >= 1) {
        break;
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    // cross line
    Line queryRegionLine = new Line(0, HEIGHT * 0.1, WIDTH, HEIGHT * 0.1);
    Line chunkGroupRegionLine = new Line(0, HEIGHT * 0.4, WIDTH, HEIGHT * 0.4);
    ObservableList<Node> children = super.root.getChildren();
    children.add(queryRegionLine);
    //        children.add(binaryRegionLine);
    children.add(chunkGroupRegionLine);

    // query region
    GridPane pan = new GridPane();
    pan.setLayoutX(0);
    pan.setLayoutY(0);
    pan.setPrefWidth(WIDTH);
    pan.setPrefHeight(HEIGHT * 0.1);
    pan.setVgap(10);
    pan.setHgap(5);
    pan.setPadding(new Insets(5));
    children.add(pan);
    Label startTime = new Label("startTime:");
    pan.add(startTime, 0, 0);
    DateTimePicker startPicker = new DateTimePicker();
    pan.add(startPicker, 1, 0);
    Label endTime = new Label("endTime:");
    pan.add(endTime, 2, 0);
    DateTimePicker endPicker = new DateTimePicker();
    pan.add(endPicker, 3, 0);

    Label deviceIdLabel = new Label("deviceId:");
    pan.add(deviceIdLabel, 4, 0);
    TextField deviceIdText = new TextField();
    pan.add(deviceIdText, 5, 0);
    Label measurementIdLabel = new Label("measurementId:");
    pan.add(measurementIdLabel, 6, 0);
    TextField measurementIdText = new TextField();
    pan.add(measurementIdText, 7, 0);
    //        Label valueLabel = new Label("value:");
    //        pan.add(valueLabel, 0, 2);
    //        TextField valueText = new TextField();
    //        pan.add(valueText, 1, 2);
    Button queryButton = new Button("search...");
    queryButton.setPrefWidth(WIDTH);
    pan.add(queryButton, 0, 1, 8, 1);
    queryButton.setOnMouseClicked(
        e -> {
          long startLocalTime =
              startPicker
                  .dateTimeProperty()
                  .getValue()
                  .atZone(ZoneId.systemDefault())
                  .toInstant()
                  .toEpochMilli();
          long endLocalTime =
              endPicker
                  .dateTimeProperty()
                  .getValue()
                  .atZone(ZoneId.systemDefault())
                  .toInstant()
                  .toEpochMilli();
          String dv = deviceIdText.getText();
          String mv = measurementIdText.getText();
          //            String vv = valueText.getText();
          try {
            QueryDataSet queryDataSet =
                this.tsFileAnalyserV13.queryResult(startLocalTime, endLocalTime, dv, mv, "", 0, 0);
            showQueryDataSet(dv, mv, queryDataSet);
          } catch (Exception ex) {
            logger.error("Failed to query data set, deviceId:{}, measurementId:{}", dv, mv);
            ex.printStackTrace();
          }
        });

    // chunk data table view init
    tableViewInit(
        pageTableView,
        pageDatas,
        false,
        genColumn(TableAlign.CENTER, "uncompressedSize", "uncompressedSize"),
        genColumn(TableAlign.CENTER, "compressedSize", "compressedSize"),
        genColumn(TableAlign.CENTER_LEFT, "statistics", "statistics"));
    pageTableView.setLayoutX(WIDTH * 0.4);
    pageTableView.setLayoutY(HEIGHT * 0.1);
    pageTableView.setPrefWidth(WIDTH * 0.6);
    pageTableView.setPrefHeight(HEIGHT * 0.1);
    tableViewInit(
        tvTableView,
        tvDatas,
        false,
        genColumn(TableAlign.CENTER, "timestamp", "timestamp"),
        genColumn(TableAlign.CENTER_LEFT, "value", "value"));
    tvTableView.setLayoutX(WIDTH * 0.4);
    tvTableView.setLayoutY(HEIGHT * 0.2);
    tvTableView.setPrefWidth(WIDTH * 0.6);
    tvTableView.setPrefHeight(HEIGHT * 0.2);
    tableViewInit(
        chunkTableView,
        chunkDatas,
        false,
        genColumn(TableAlign.CENTER, "dataSize", "dataSize"),
        genColumn(TableAlign.CENTER, "dataType", "dataType"),
        genColumn(TableAlign.CENTER, "compression", "compression"),
        genColumn(TableAlign.CENTER, "encoding", "encoding"));
    chunkTableView.setLayoutX(WIDTH * 0.4);
    chunkTableView.setLayoutY(HEIGHT * 0.1);
    chunkTableView.setPrefWidth(WIDTH * 0.6);
    chunkTableView.setPrefHeight(HEIGHT * 0.3);
    fileTableView = new TableView();
    ObservableList<FileInfo> fileDatas = FXCollections.observableArrayList();
    tableViewInit(
        fileTableView,
        fileDatas,
        false,
        genColumn(TableAlign.CENTER, "fileVersion", "fileVersion"),
        genColumn(TableAlign.CENTER, "fileSize(M)", "fileSize"),
        genColumn(TableAlign.CENTER, "dataCounts", "dataCounts"));
    fileTableView.setLayoutX(WIDTH * 0.4);
    fileTableView.setLayoutY(HEIGHT * 0.1);
    fileTableView.setPrefWidth(WIDTH * 0.6);
    fileTableView.setPrefHeight(HEIGHT * 0.1);
    fileDatas.add(
        new FileInfo(
            3,
            (long) (this.tsFileAnalyserV13.getFileSize() / 1024),
            this.tsFileAnalyserV13.getAllCount()));
    // chunk group init
    rootItem = new TreeItem(new ChunkTreeItemValue("Chunk Group", TREE_ITEM_TYPE_ROOT, null));
    tree = new TreeView(rootItem);
    tree.setLayoutX(0);
    tree.setLayoutY(HEIGHT * 0.1);
    tree.setPrefWidth(WIDTH * 0.4);
    tree.setPrefHeight(HEIGHT * 0.3);
    // tree listener
    tree.getSelectionModel()
        .selectedItemProperty()
        .addListener(
            (observable, oldValue, newValue) -> {
              TreeItem<ChunkTreeItemValue> value = observable.getValue();
              String type = value.getValue().getType();
              if (TREE_ITEM_TYPE_CHUNK.equals(type)) {
                showItemChunk(value);
              } else if (TREE_ITEM_TYPE_CHUNK_PAGE.equals(type)) {
                showPageDetail(value);
              } else if (TREE_ITEM_TYPE_ROOT.equals(type)) {
                this.fileTableView.setVisible(true);
                this.chunkTableView.setVisible(false);
                this.tvTableView.setVisible(false);
                this.pageTableView.setVisible(false);
              } else {
                this.fileTableView.setVisible(false);
                this.chunkTableView.setVisible(false);
                this.tvTableView.setVisible(false);
                this.pageTableView.setVisible(false);
              }
            });
    // chun group data set
    chunkGroupTreeDataInit();
    super.root.getChildren().add(tree);

    // index
    //        indexRegion = new ScrollRegion(super.root, WIDTH * 1, HEIGHT * 0.6, 0, HEIGHT * 0.4);
    ScrollPane indexRegion = new ScrollPane();
    indexGroup = new Group();
    indexRegion.setContent(indexGroup);
    indexRegion.setLayoutX(0);
    indexRegion.setLayoutY(HEIGHT * 0.4);
    indexRegion.setPrefWidth(WIDTH);
    indexRegion.setPrefHeight(HEIGHT * 0.6);
    root.getChildren().add(indexRegion);
    indexDataInit();

    // search hidden button
    Button searchHiddenButton = new Button("searchHiddenButton");
    searchHiddenButton.setVisible(false);
    this.root.getChildren().add(searchHiddenButton);

    // shorcut key binding: CTR+F
    KeyCombination kccb = new KeyCodeCombination(KeyCode.F, KeyCombination.CONTROL_DOWN);
    this.getScene().getAccelerators().put(kccb, ()-> searchHiddenButton.fire());

    // 为按钮添加事件——触发快捷键时打开新的窗口
    searchHiddenButton.setOnAction(event ->  {
      // 创建新的stage
      Stage secondStage = new Stage();
      timeseriesSearchPage = new TimeseriesSearchPage(secondStage, this);

      secondStage.show();
    });
  }

  public TsFileAnalyserV13 getTsFileAnalyserV13() {
    return tsFileAnalyserV13;
  }

  public String timeseriesSearch(String searchText) {
    for (String timeseriesStr : timeseriesList) {
      if (timeseriesStr.contains(searchText)) {
        return timeseriesStr;
      }
    }
    return "";
  }

  private void showQueryDataSet(String deviceId, String measurementId, QueryDataSet queryDataSet) throws Exception {

    // select root
    this.tree.getSelectionModel().select(0);
    // show datas
    this.tvDatas.clear();
    this.pageDatas.clear();
    this.chunkTableView.setVisible(false);
    while (queryDataSet.hasNext()) {
      RowRecord next = queryDataSet.next();
      StringBuilder sb = new StringBuilder();
      for (Field f : next.getFields()) {
        sb.append("\t");
        sb.append(f);
      }
      this.tvDatas.add(new TimesValues(new Date(next.getTimestamp()).toString(), sb.toString()));
    }
    this.tvTableView.setVisible(true);
    this.pageTableView.setVisible(true);
  }

  /**
   * click index meta show tree item
   *
   * @param path
   */
  public void chooseTree(String path) {
    TreeItem<ChunkTreeItemValue> chunkTreeItemValueTreeItem = this.indexMap.get(path);
    if (chunkTreeItemValueTreeItem != null) {
      tree.getSelectionModel().select(chunkTreeItemValueTreeItem);
      int index = tree.getSelectionModel().selectedIndexProperty().get();
      tree.scrollTo(index);
    }
  }

  /** index tree init */
  private void indexDataInit() {
    try {
      TimeSeriesMetadataNode timeSeriesMetadataNode =
          this.tsFileAnalyserV13.getTimeSeriesMetadataNode();
      if (timeSeriesMetadataNode == null) {
        throw new Exception("index is null");
      }
      IndexNode indexNode = new IndexNode(timeSeriesMetadataNode, null, this.indexGroup, this);
      indexNode.draw();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * click chunk item show detail
   *
   * @param value
   */
  public void showItemChunk(TreeItem<ChunkTreeItemValue> value) {
    this.tvTableView.setVisible(false);
    this.chunkDatas.clear();
    this.pageTableView.setVisible(false);
    this.fileTableView.setVisible(false);
    ChunkWrap params = (ChunkWrap) value.getValue().getParams();
    ChunkHeader chunkHeader = params.getChunkHeader();
    this.chunkDatas.add(
        new ChunkInfo(
            chunkHeader.getDataSize(),
            chunkHeader.getDataType().toString(),
            chunkHeader.getCompressionType().toString(),
            chunkHeader.getEncodingType().toString()));
    this.chunkTableView.setVisible(true);
    try {
      List<org.apache.iotdb.tool.core.model.PageInfo> pageInfoList =
          this.tsFileAnalyserV13.fetchPageInfoListByChunkMetadata(params.getChunkMetadata());
      ObservableList<TreeItem<ChunkTreeItemValue>> chunkChild = value.getChildren();
      if (chunkChild == null) {
        return;
      }
      if (chunkChild.size() == 0) {
        if (pageInfoList != null && pageInfoList.get(0) != null) {
          for (int i = 1; i <= pageInfoList.size(); i++) {
            ChunkTreeItemValue pageValue =
                new ChunkTreeItemValue("page " + i, TREE_ITEM_TYPE_CHUNK_PAGE, pageInfoList.get(i - 1));
            TreeItem<ChunkTreeItemValue> pageItem = new TreeItem<>(pageValue);
            chunkChild.add(pageItem);
          }
        }
      }
    } catch (IOException e) {
      logger.error(
          "Failed to get pageInfo list of the chunk, the chunk dataType:{}",
          params.getChunkMetadata().getDataType());
      e.printStackTrace();
      return;
    }
  }

  /**
   * click page item show detail
   *
   * @param value
   */
  private void showPageDetail(TreeItem<ChunkTreeItemValue> value) {
    this.tvDatas.clear();
    this.pageDatas.clear();
    this.fileTableView.setVisible(false);
    this.chunkTableView.setVisible(false);
    org.apache.iotdb.tool.core.model.PageInfo pageInfo =
        (org.apache.iotdb.tool.core.model.PageInfo) value.getValue().getParams();
    try {
      if (pageInfo != null) {
        this.pageDatas.add(
            new PageInfo(
                pageInfo.getUncompressedSize(),
                pageInfo.getCompressedSize(),
                pageInfo.getStatistics() == null ? "" : pageInfo.getStatistics().toString()));
      }
      BatchData batchData = this.tsFileAnalyserV13.fetchBatchDataByPageInfo(pageInfo);
      while (batchData.hasCurrent()) {
        Object currValue = batchData.currentValue();
        this.tvDatas.add(
            new TimesValues(
                new Date(batchData.currentTime()).toString(),
                currValue == null ? "" : currValue.toString()));
        batchData.next();
      }
    } catch (Exception e) {
      logger.error(
          "Failed to get page details, the page statistics:{}",
          pageInfo.getStatistics().toString());
      e.printStackTrace();
    }
    this.tvTableView.setVisible(true);
    this.pageTableView.setVisible(true);
  }

  private void tableViewInit(
      TableView tableView, ObservableList datas, boolean isShow, TableColumn... genColumn) {

    tableView.setItems(datas);
    tableView.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
    super.root.getChildren().add(tableView);
    tableView.getColumns().addAll(genColumn);
    tableView.setVisible(isShow);
  }

  /** chunk group tree data set */
  private void chunkGroupTreeDataInit() {

    List<ChunkGroupMetadataModel> chunkGroupMetadataModelList =
        this.tsFileAnalyserV13.getChunkGroupMetadataModelList();
    if (chunkGroupMetadataModelList == null) {
      return;
    }
    chunkGroupMetadataModelList.forEach(
        chunkGroupMetadataMode -> {
          ChunkTreeItemValue chunkGroupMetaItemValue =
              new ChunkTreeItemValue(
                  chunkGroupMetadataMode.getDevice(),
                  TREE_ITEM_TYPE_CHUNK_GROUP,
                  chunkGroupMetadataMode);
          TreeItem<ChunkTreeItemValue> chunkGroupMetaItem = new TreeItem<>(chunkGroupMetaItemValue);
          rootItem.getChildren().add(chunkGroupMetaItem);
          timeseriesList.add(chunkGroupMetaItemValue.getName());
          indexMap.put(chunkGroupMetadataMode.getDevice(), chunkGroupMetaItem);
          List<ChunkMetadata> chunkMetadataList = chunkGroupMetadataMode.getChunkMetadataList();
          List<ChunkHeader> chunkHeaderList = chunkGroupMetadataMode.getChunkHeaderList();
          if (chunkMetadataList != null
              && chunkHeaderList != null
              && chunkMetadataList.size() == chunkHeaderList.size()) {
            for (int i = 0; i < chunkMetadataList.size(); i++) {
              ChunkMetadata chunkMetadata = chunkMetadataList.get(i);
              ChunkHeader chunkHeader = chunkHeaderList.get(i);
              ChunkTreeItemValue chunkMetaItemValue =
                  new ChunkTreeItemValue(
                      chunkMetadata.getMeasurementUid(),
                      TREE_ITEM_TYPE_CHUNK,
                      new ChunkWrap(chunkMetadata, chunkHeader));
              TreeItem<ChunkTreeItemValue> chunkMetaItem = new TreeItem<>(chunkMetaItemValue);
              chunkGroupMetaItem.getChildren().add(chunkMetaItem);
              timeseriesList.add(chunkGroupMetaItemValue.getName() + "." + chunkMetaItemValue.getName());
              indexMap.put(
                  chunkGroupMetadataMode.getDevice() + "." + chunkMetadata.getMeasurementUid(),
                  chunkMetaItem);
            }
          }
        });
  }

  private TableColumn genColumn(TableAlign align, String showName, String name) {

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
                if (item == getItem()) return;
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
                }
              }
            };
          }
        });
    column.setStyle("-fx-alignment: " + align.getAlign() + ";");
    return column;
  }

  @Override
  public String getName() {
    return "TsFileV0.13";
  }

  /** time value table */
  public static class TimesValues {

    private final SimpleStringProperty timestamp;
    private final SimpleStringProperty value;

    public TimesValues(String timestamp, String value) {
      this.timestamp = new SimpleStringProperty(timestamp);
      this.value = new SimpleStringProperty(value);
    }

    public String getTimestamp() {
      return timestamp.get();
    }

    public SimpleStringProperty timestampProperty() {
      return timestamp;
    }

    public void setTimestamp(String timestamp) {
      this.timestamp.set(timestamp);
    }

    public String getValue() {
      return value.get();
    }

    public SimpleStringProperty valueProperty() {
      return value;
    }

    public void setValue(String value) {
      this.value.set(value);
    }
  }

  /** chunk info table */
  public static class ChunkInfo {

    // ----measurmentId:targetHost,dataSize:10224,dataType:TEXT,compression:SNAPPY,encoding:PLAIN
    private final SimpleIntegerProperty dataSize;
    private final SimpleStringProperty dataType;
    private final SimpleStringProperty compression;
    private final SimpleStringProperty encoding;

    public ChunkInfo(int dataSize, String dataType, String compression, String encoding) {
      this.dataSize = new SimpleIntegerProperty(dataSize);
      this.dataType = new SimpleStringProperty(dataType);
      this.compression = new SimpleStringProperty(compression);
      this.encoding = new SimpleStringProperty(encoding);
    }

    public int getDataSize() {
      return dataSize.get();
    }

    public SimpleIntegerProperty dataSizeProperty() {
      return dataSize;
    }

    public void setDataSize(int dataSize) {
      this.dataSize.set(dataSize);
    }

    public String getDataType() {
      return dataType.get();
    }

    public SimpleStringProperty dataTypeProperty() {
      return dataType;
    }

    public void setDataType(String dataType) {
      this.dataType.set(dataType);
    }

    public String getCompression() {
      return compression.get();
    }

    public SimpleStringProperty compressionProperty() {
      return compression;
    }

    public void setCompression(String compression) {
      this.compression.set(compression);
    }

    public String getEncoding() {
      return encoding.get();
    }

    public SimpleStringProperty encodingProperty() {
      return encoding;
    }

    public void setEncoding(String encoding) {
      this.encoding.set(encoding);
    }
  }

  /** time value table */
  public static class PageInfo {

    private final SimpleIntegerProperty uncompressedSize;
    private final SimpleIntegerProperty compressedSize;
    private final SimpleStringProperty statistics;

    public PageInfo(int uncompressedSize, int compressedSize, String statistics) {
      this.uncompressedSize = new SimpleIntegerProperty(uncompressedSize);
      this.compressedSize = new SimpleIntegerProperty(compressedSize);
      this.statistics = new SimpleStringProperty(statistics);
    }

    public int getUncompressedSize() {
      return uncompressedSize.get();
    }

    public SimpleIntegerProperty uncompressedSizeProperty() {
      return uncompressedSize;
    }

    public void setUncompressedSize(int uncompressedSize) {
      this.uncompressedSize.set(uncompressedSize);
    }

    public int getCompressedSize() {
      return compressedSize.get();
    }

    public SimpleIntegerProperty compressedSizeProperty() {
      return compressedSize;
    }

    public void setCompressedSize(int compressedSize) {
      this.compressedSize.set(compressedSize);
    }

    public String getStatistics() {
      return statistics.get();
    }

    public SimpleStringProperty statisticsProperty() {
      return statistics;
    }

    public void setStatistics(String statistics) {
      this.statistics.set(statistics);
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

  /**
   * table align [ top-left | top-center | top-right | center-left | center | center-right
   * bottom-left | bottom-center | bottom-right | baseline-left | baseline-center | baseline-right ]
   */
  public enum TableAlign {
    CENTER("CENTER"),
    CENTER_LEFT("center-left");

    String align;

    TableAlign(String align) {
      this.align = align;
    }

    public String getAlign() {
      return align;
    }

    public void setAlign(String align) {
      this.align = align;
    }
  }

  public class ChunkTreeItemValue {

    private String name;
    private String type;
    private Object params;

    public ChunkTreeItemValue(String name, String type, Object params) {
      this.name = name;
      this.type = type;
      this.params = params;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public Object getParams() {
      return params;
    }

    public void setParams(Object params) {
      this.params = params;
    }

    @Override
    public String toString() {
      return getName();
    }
  }

  public static class ChunkWrap {
    public ChunkMetadata chunkMetadata;
    public ChunkHeader chunkHeader;

    public ChunkWrap(ChunkMetadata chunkMetadata, ChunkHeader chunkHeader) {
      this.chunkMetadata = chunkMetadata;
      this.chunkHeader = chunkHeader;
    }

    public ChunkMetadata getChunkMetadata() {
      return chunkMetadata;
    }

    public void setChunkMetadata(ChunkMetadata chunkMetadata) {
      this.chunkMetadata = chunkMetadata;
    }

    public ChunkHeader getChunkHeader() {
      return chunkHeader;
    }

    public void setChunkHeader(ChunkHeader chunkHeader) {
      this.chunkHeader = chunkHeader;
    }
  }
}
