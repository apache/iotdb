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

import org.apache.iotdb.tool.core.model.*;
import org.apache.iotdb.tool.core.service.TsFileAnalyserV13;
import org.apache.iotdb.tool.ui.node.IndexNode;
import org.apache.iotdb.tool.ui.view.IconView;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleLongProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.ObservableList;
import javafx.scene.Group;
import javafx.scene.Node;
import javafx.scene.control.*;
import javafx.scene.image.ImageView;
import javafx.scene.input.*;
import javafx.scene.layout.*;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.StageStyle;

import static org.apache.iotdb.tool.ui.common.constant.TSFileMTConstant.*;

/**
 * IoTDBParsePage
 *
 * @author oortCloudFei
 */
public class IoTDBParsePageV3 extends IoTDBParsePage {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBParsePageV3.class);
  public static final double WIDTH = 1080;
  public static final double HEIGHT = 750;
  private static final String TREE_ITEM_TYPE_CHUNK_GROUP = "cg";
  private static final String TREE_ITEM_TYPE_CHUNK = "c";
  private static final String TREE_ITEM_TYPE_CHUNK_PAGE = "cp";
  private static final String TREE_ITEM_TYPE_FOLDER = "folder";
  private static final String TREE_ITEM_TYPE_TSFILE = "tsfile";

  private static final String ALIGNED_CHUNK = "Aligned Chunk";

  /** version 13 interface */
  private TsFileAnalyserV13 tsFileAnalyserV13;

  /** index region */
  private Group indexGroup;

  /** TsFile Item */
  private TreeItem tsfileItem;

  private TreeItem preTSFileItem;

  private TreeView<ChunkTreeItemValue> treeView;

  private ContextMenu treeViewMenu;

  /** click index to tree */
  private Map<String, TreeItem<ChunkTreeItemValue>> indexMap = new HashMap<>(256);

  private List<String> timeseriesList = new ArrayList<>();

  /** measurementSearch Stage */
  private MeasurementSearchPage measurementSearchPage;

  private TsFileInfoPage tsfileInfoPage;

  private ChunkInfoPage chunkInfoPage;

  private PageInfoPage pageInfoPage;

  private TsFileLoadPage tsFileLoadPage;

  private EncodeAnalysePage encodeAnalysePage;

  private File selectedFolder;

  private Stage tsfileLoadStage;

  private String loadedTSFileName;

  public IoTDBParsePageV3() {
    super(new Group(), WIDTH, HEIGHT);
    tsFileLoadPage = new TsFileLoadPage();
  }

  public void setTsFileAnalyserV13(TsFileAnalyserV13 tsFileAnalyserV13) {
    this.tsFileAnalyserV13 = tsFileAnalyserV13;
  }

  public void init(Stage baseStage) {
    // TreeView Region
    treeView = new TreeView<ChunkTreeItemValue>();
    treeView.setLayoutX(0);
    treeView.setLayoutY(HEIGHT * 0.04);
    treeView.setPrefWidth(WIDTH * 0.3);
    treeView.setPrefHeight(HEIGHT * 0.93);
    this.root.getChildren().add(treeView);

    // tree listener
    treeView
        .getSelectionModel()
        .selectedItemProperty()
        .addListener(
            (observable, oldValue, newValue) -> {
              TreeItem<ChunkTreeItemValue> treeItem = observable.getValue();
              String type = null;
              if (treeItem != null) {
                type = treeItem.getValue().getType();
              }
              // switch
              switch (type) {
                case TREE_ITEM_TYPE_CHUNK:
                  if (ALIGNED_CHUNK.equals(treeItem.getValue().getName())) {
                    showItemAlignedChunk(treeItem);
                  } else {
                    showItemChunk(treeItem);
                  }
                  break;
                case TREE_ITEM_TYPE_CHUNK_GROUP:
                  try {
                    showItemChunkGroup(treeItem);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                  break;
                default:
                  logger.info("can not support item type:{}", type);
              }
            });

    // TreeView Menu
    treeViewMenu = new ContextMenu();
    ObservableList<MenuItem> treeViewItems = treeViewMenu.getItems();

    treeView
        .getSelectionModel()
        .selectedItemProperty()
        .addListener(
            (observable, oldValue, newValue) -> {
              TreeItem<ChunkTreeItemValue> currItem =
                  treeView.getSelectionModel().getSelectedItem();
              if (currItem == null) {
                return;
              }
              if (!ALIGNED_CHUNK.equals(currItem.getValue().getName())) {
                treeViewItems.clear();
                String type = currItem.getValue().getType();
                switch (type) {
                  case TREE_ITEM_TYPE_TSFILE:
                    // open tsfile
                    MenuItem openTSFileItem = new MenuItem("open tsfile");
                    treeViewItems.add(openTSFileItem);
                    openTSFileItem.setOnAction(
                        event -> {
                          openTSFile();
                        });

                    // 判断是否已经加载，若未加载不显示 tsfile details
                    String currTSFileName = currItem.getValue().getName();
                    if (currTSFileName != null
                        && loadedTSFileName != null
                        && currTSFileName == loadedTSFileName) {
                      // tsfile details item
                      MenuItem tsfileMenuItem = new MenuItem("tsfile details");
                      treeViewItems.add(tsfileMenuItem);
                      tsfileMenuItem.setOnAction(
                          event -> {
                            Stage tsfileInfoStage = new Stage();
                            tsfileInfoStage.initStyle(StageStyle.UTILITY);
                            tsfileInfoStage.initModality(Modality.APPLICATION_MODAL);
                            tsfileInfoPage =
                                new TsFileInfoPage(tsfileInfoStage, this, currTSFileName);
                          });
                    }
                    break;
                  case TREE_ITEM_TYPE_CHUNK:
                    MenuItem chunkMenuItem = new MenuItem("chunk details");
                    treeViewItems.add(chunkMenuItem);
                    chunkMenuItem.setOnAction(
                        event -> {
                          Stage chunkInfoStage = new Stage();
                          chunkInfoStage.initStyle(StageStyle.UTILITY);
                          chunkInfoStage.initModality(Modality.APPLICATION_MODAL);
                          chunkInfoPage = new ChunkInfoPage(chunkInfoStage, this, currItem);
                        });
                    break;
                  case TREE_ITEM_TYPE_CHUNK_PAGE:
                    MenuItem pageMenuItem = new MenuItem("page details");
                    treeViewItems.add(pageMenuItem);
                    pageMenuItem.setOnAction(
                        event -> {
                          Stage pageInfoStage = new Stage();
                          pageInfoStage.initStyle(StageStyle.UTILITY);
                          pageInfoStage.initModality(Modality.APPLICATION_MODAL);
                          pageInfoPage = new PageInfoPage(pageInfoStage, this, currItem);
                        });
                    break;
                  default:
                    logger.info("unexpect type:{}", type);
                }
              }
            });
    treeView.setContextMenu(treeViewMenu);

    // menu region
    MenuBar menuBar = new MenuBar();
    menuBar.prefWidthProperty().bind(baseStage.widthProperty());
    this.root.getChildren().add(menuBar);
    menuBar.setLayoutX(0);
    menuBar.setLayoutY(0);
    menuBar.setPrefHeight(HEIGHT * 0.03);
    // load file
    Menu fileMenu = new Menu("File");
    MenuItem loadFileMenuItem = new MenuItem("Load Dir");
    fileMenu.getItems().addAll(loadFileMenuItem);
    loadFileMenuItem.setOnAction(
        event -> {
          selectedFolder = tsFileLoadPage.loadFolder(baseStage);
          if (selectedFolder != null) {
            TreeItem<ChunkTreeItemValue> treeRoot =
                new TreeItem<>(
                    new ChunkTreeItemValue(selectedFolder.getName(), TREE_ITEM_TYPE_FOLDER, null));
            Node folderIcon = new IconView("/icons/folder-package.png");
            treeRoot.setGraphic(folderIcon);

            File[] files =
                selectedFolder.listFiles(
                    ((dir, name) -> {
                      if (name.toLowerCase().endsWith(".tsfile")) {
                        File file = new File(dir + "\\" + name);
                        if (file.isDirectory()) {
                          return false;
                        }
                        return true;
                      }
                      return false;
                    }));
            if (files == null || files.length == 0) {
              logger.error("The File[] files is null!");
              return;
            }
            for (File file : files) {
              String filePath = file.getPath();
              TreeItem<ChunkTreeItemValue> fileItem =
                  new TreeItem<>(
                      new ChunkTreeItemValue(file.getName(), TREE_ITEM_TYPE_TSFILE, filePath));
              treeRoot.getChildren().add(fileItem);
              Node tsfileIcon = new IconView("/icons/folder-source.png");
              fileItem.setGraphic(tsfileIcon);
            }
            treeView.setRoot(treeRoot);
            treeRoot.setExpanded(true);
            this.clearParsePageCache();
          }
        });

    Menu searchMenu = new Menu("Search");
    CheckMenuItem searchMenuItem = new CheckMenuItem("Search Measurements");
    searchMenu.getItems().addAll(searchMenuItem);
    Menu encodeMenu = new Menu("Encode & Compress");
    CheckMenuItem encodeAnalyseMenuItem = new CheckMenuItem("Encode & Compress Analyse");
    encodeMenu.getItems().addAll(encodeAnalyseMenuItem);
    Menu configMenu = new Menu("Config");
    Menu helpManeu = new Menu("Help");
    helpManeu.getItems().addAll(new CheckMenuItem("Documentation"), new CheckMenuItem("Contact"));
    menuBar.getMenus().addAll(fileMenu, searchMenu, encodeMenu, configMenu, helpManeu);

    // Measurement Search
    searchMenuItem.setSelected(false);
    searchMenuItem.setOnAction(
        event -> {
          if (measurementSearchPage != null && measurementSearchPage.isShow()) {
            return;
          }
          if (measurementSearchPage != null) {
            measurementSearchPage.close();
          }
          Stage measurementSearchStage = new Stage();
          measurementSearchStage.initStyle(StageStyle.UTILITY);
          measurementSearchPage = new MeasurementSearchPage(measurementSearchStage, this);
        });

    // TimeSeries search
    HBox searchHBox = new HBox();
    TextField searchText = new TextField();
    searchText.setPromptText("Please enter DeviceId or MeasurementId");
    searchText.getStyleClass().add("search-field");
    searchText.setOnKeyReleased(
        event -> {
          String keyCodeName = event.getCode().getName();
          switch (keyCodeName) {
            case KEYCODE_ESCAPE:
              searchText.setText("");
              break;
            case KEYCODE_UP:
            case KEYCODE_DOWN:
              treeView.requestFocus();
              break;
            default:
              logger.warn("Unexpected keycode value{}:", keyCodeName);
          }
        });
    Button searchButton = new Button();
    searchButton.setGraphic(new ImageView("/icons/find-light.png"));
    searchButton.getStyleClass().add("search-button");
    searchHBox.getChildren().addAll(searchText, searchButton);
    searchText.setPrefWidth(WIDTH * 0.27);
    searchHBox.setLayoutX(0);
    searchHBox.setLayoutY(HEIGHT * 0.968);
    this.root.getChildren().add(searchHBox);
    // search hidden button
    Button searchHiddenButton = new Button("searchHiddenButton");
    searchHiddenButton.setManaged(false);
    this.root.getChildren().add(searchHiddenButton);
    // shortcut key binding: CTR + F
    KeyCombination shButtonKC = new KeyCodeCombination(KeyCode.F, KeyCombination.SHORTCUT_DOWN);
    this.getScene().getAccelerators().put(shButtonKC, searchHiddenButton::fire);
    // hiddenButton: focus event
    searchHiddenButton.setOnAction(
        event -> {
          searchText.requestFocus();
          searchText.selectAll();
        });
    // searchButton: ENTER shortcut binding
    KeyCombination sButtonKC = new KeyCodeCombination(KeyCode.ENTER);
    this.getScene().getAccelerators().put(sButtonKC, searchButton::fire);
    // searchButton: search event
    searchButton.setOnAction(
        event -> {
          String searchResult = timeseriesSearch(searchText.getText().trim());
          if (searchResult == null) {
            return;
          }
          chooseTree(searchResult);
        });

    // Encode and Compress Analyse
    encodeAnalyseMenuItem.setSelected(false);
    encodeAnalyseMenuItem.setOnAction(
        event -> {
          if (encodeAnalysePage != null && encodeAnalysePage.isShow()) {
            return;
          }
          if (encodeAnalysePage != null) {
            encodeAnalysePage.close();
          }
          Stage encodeAnalyseStage = new Stage();
          encodeAnalyseStage.initStyle(StageStyle.UTILITY);
          encodeAnalysePage = new EncodeAnalysePage(encodeAnalyseStage, this);
        });

    // index region
    ScrollPane indexRegion = new ScrollPane();
    indexGroup = new Group();
    indexRegion.setContent(indexGroup);
    indexRegion.setLayoutX(WIDTH * 0.3);
    indexRegion.setLayoutY(HEIGHT * 0.04);
    indexRegion.setPrefWidth(WIDTH * 0.7);
    indexRegion.setPrefHeight(HEIGHT * 0.96);
    root.getChildren().add(indexRegion);

    URL uiDarkCssResource = getClass().getClassLoader().getResource("css/ui-dark.css");
    if (uiDarkCssResource != null) {
      this.getScene().getStylesheets().add(uiDarkCssResource.toExternalForm());
    }
  }

  public TsFileAnalyserV13 getTsFileAnalyserV13() {
    return tsFileAnalyserV13;
  }

  private void openTSFile() {
    // create new stage
    TreeItem<ChunkTreeItemValue> currItem = treeView.getSelectionModel().getSelectedItem();
    if (currItem != null) {
      String type = currItem.getValue().getType();
      if (TREE_ITEM_TYPE_TSFILE.equals(type)) {
        String filePath = (String) currItem.getValue().getParams();
        // 0. 是否重复加载
        if (loadedTSFileName != null && currItem.getValue().getName().equals(loadedTSFileName)) {
          Alert alert =
              new Alert(
                  Alert.AlertType.ERROR, "Cannot open the same file repeatedly!", ButtonType.OK);
          alert.showAndWait();
          return;
        }
        // 1. file type check
        if (!tsFileLoadPage.fileTypeCheck(filePath)) {
          Alert alert = new Alert(Alert.AlertType.ERROR, "Please choose TsFile!", ButtonType.OK);
          alert.showAndWait();
          return;
        }
        // 2. file version check
        if (!tsFileLoadPage.fileVersionCheck(filePath)) {
          Alert alert =
              new Alert(
                  Alert.AlertType.ERROR,
                  "Sorry, We currently only support the 3.0 TsFile version!",
                  ButtonType.OK);
          alert.showAndWait();
          return;
        }
        tsfileLoadStage = new Stage();
        tsfileLoadStage.initStyle(StageStyle.TRANSPARENT);
        tsfileLoadStage.initModality(Modality.APPLICATION_MODAL);
        tsfileLoadStage.show();
        // 3. load file 初始化, 实际上在弹窗 load
        // 应记录 preTSFileItem, 方便后续清空缓存
        if (tsfileItem != null) {
          preTSFileItem = tsfileItem;
        }
        tsfileItem = currItem;
        tsFileLoadPage = new TsFileLoadPage(tsfileLoadStage, filePath);
        tsFileLoadPage.setIoTDBParsePageV13(IoTDBParsePageV3.this);
      }
    }
  }

  public String timeseriesSearch(String searchText) {
    for (String timeseriesStr : timeseriesList) {
      if (timeseriesStr.contains(searchText)) {
        return timeseriesStr;
      }
    }
    return "";
  }

  /**
   * click index meta show tree item
   *
   * @param path
   */
  public void chooseTree(String path) {
    TreeItem<ChunkTreeItemValue> chunkTreeItemValueTreeItem = this.indexMap.get(path);
    if (chunkTreeItemValueTreeItem != null) {
      treeView.getSelectionModel().select(chunkTreeItemValueTreeItem);
      int index = treeView.getSelectionModel().selectedIndexProperty().get();
      treeView.scrollTo(index);
    }
  }

  /** index tree init */
  private void indexDataInit() {
    try {
      long indexRegionStartTime = System.currentTimeMillis();

      TimeSeriesMetadataNode timeSeriesMetadataNode =
          this.tsFileAnalyserV13.getTimeSeriesMetadataNode();
      if (timeSeriesMetadataNode == null) {
        logger.error("index is null !");
        return;
      }
      IndexNode indexNode = new IndexNode(timeSeriesMetadataNode, null, this.indexGroup, this);
      indexNode.draw();

      long indexRegionEndTime = System.currentTimeMillis();
      System.out.println(
          "index Region total time cost: " + (indexRegionEndTime - indexRegionStartTime));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void showItemChunkGroup(TreeItem<ChunkTreeItemValue> chunkGroupItem) throws IOException {
    long offset = (long) chunkGroupItem.getValue().getParams();
    ChunkListInfo chunkListInfo = tsFileAnalyserV13.fetchChunkListByChunkGroupOffset(offset);
    // 1. 获取 List<IChunkMetadata>
    List<IChunkMetadata> chunkMetadataList = chunkListInfo.getChunkMetadataList();
    // 2. 获取 List<ChunkHeader> chunkHeaderLists
    List<ChunkHeader> chunkHeaderList = chunkListInfo.getChunkHeaderList();

    ObservableList<TreeItem<ChunkTreeItemValue>> chunkGroupChildren = chunkGroupItem.getChildren();
    if (chunkGroupChildren == null) {
      return;
    }
    // 此 ChunkGroup 未加载过 Chunk
    if (chunkGroupChildren.size() == 0) {
      if (chunkMetadataList != null
          && !chunkMetadataList.isEmpty()
          && chunkHeaderList != null
          && !chunkHeaderList.isEmpty()) {
        ChunkTreeItemValue chunkMetaItemValue = null;
        if (chunkMetadataList.get(0) != null) {
          // 0. Aligned Chunk (虚拟 Chunk)
          if (chunkHeaderList.get(0).getDataType() == TSDataType.VECTOR) {
            chunkMetaItemValue =
                new ChunkTreeItemValue(
                    ALIGNED_CHUNK,
                    TREE_ITEM_TYPE_CHUNK,
                    new AlignedChunkWrap(chunkMetadataList, chunkHeaderList));
            TreeItem<ChunkTreeItemValue> chunkMetaItem = new TreeItem<>(chunkMetaItemValue);
            Node measurementIcon = new IconView("icons/text-code.png");
            chunkMetaItem.setGraphic(measurementIcon);
            chunkGroupChildren.add(chunkMetaItem);
          }
          // non-aligned
          if (chunkHeaderList.get(0).getDataType() != TSDataType.VECTOR) {
            for (int i = 0; i < chunkMetadataList.size(); i++) {
              IChunkMetadata iChunkMetadata = chunkMetadataList.get(i);
              ChunkHeader chunkHeader = chunkHeaderList.get(i);
              chunkMetaItemValue =
                  new ChunkTreeItemValue(
                      iChunkMetadata.getMeasurementUid(),
                      TREE_ITEM_TYPE_CHUNK,
                      new ChunkWrap(iChunkMetadata, chunkHeader));
              TreeItem<ChunkTreeItemValue> chunkMetaItem = new TreeItem<>(chunkMetaItemValue);
              Node measurementIcon = new IconView("icons/text-code.png");
              chunkMetaItem.setGraphic(measurementIcon);
              chunkGroupChildren.add(chunkMetaItem);
              // 添加检索信息
              timeseriesList.add(
                  chunkGroupItem.getValue().getName() + "." + chunkMetaItemValue.getName());
              indexMap.put(
                  chunkGroupItem.getValue().getName() + "." + chunkMetaItemValue.getName(),
                  chunkMetaItem);
            }
          }
        }
      }
    }
  }

  /**
   * click chunk item show detail
   *
   * @param chunkItem
   */
  public void showItemChunk(TreeItem<ChunkTreeItemValue> chunkItem) {
    ChunkWrap params = (ChunkWrap) chunkItem.getValue().getParams();
    try {
      List<org.apache.iotdb.tool.core.model.IPageInfo> pageInfoList =
          tsFileAnalyserV13.fetchPageInfoListByChunkMetadata(params.getiChunkMetadata());
      ObservableList<TreeItem<ChunkTreeItemValue>> chunkChild = chunkItem.getChildren();
      if (chunkChild == null) {
        return;
      }
      if (chunkChild.size() == 0) {
        if (pageInfoList != null && pageInfoList.get(0) != null) {
          for (int i = 1; i <= pageInfoList.size(); i++) {
            ChunkTreeItemValue pageValue =
                new ChunkTreeItemValue(
                    "page " + i, TREE_ITEM_TYPE_CHUNK_PAGE, pageInfoList.get(i - 1));
            TreeItem<ChunkTreeItemValue> pageItem = new TreeItem<>(pageValue);
            Node pageIcon = new IconView("/icons/text.png");
            pageItem.setGraphic(pageIcon);
            chunkChild.add(pageItem);
          }
        }
      }
    } catch (IOException e) {
      logger.error(
          "Failed to get pageInfo list of the chunk, the chunk dataType:{}",
          params.getiChunkMetadata().getDataType());
    }
  }

  /** click Aligned Chunk show its pages */
  public void showItemAlignedChunk(TreeItem<ChunkTreeItemValue> alignedChunkItem) {
    AlignedChunkWrap params = (AlignedChunkWrap) alignedChunkItem.getValue().getParams();
    List<ChunkHeader> chunkHeaderList = params.getChunkHeaderList();
    try {
      List<org.apache.iotdb.tool.core.model.IPageInfo> pageInfoLists =
          tsFileAnalyserV13.fetchPageInfoListByIChunkMetadata(params.getChunkMetadataList().get(0));
      ObservableList<TreeItem<ChunkTreeItemValue>> chunkChild = alignedChunkItem.getChildren();
      if (chunkChild == null) {
        return;
      }
      if (chunkChild.size() == 0) {
        if (pageInfoLists != null && pageInfoLists.get(0) != null) {
          for (int i = 1; i <= pageInfoLists.size(); i++) {
            AlignedPageItemParams pageItemParams =
                new AlignedPageItemParams(pageInfoLists.get(i - 1), chunkHeaderList);
            ChunkTreeItemValue pageValue =
                new ChunkTreeItemValue("page " + i, TREE_ITEM_TYPE_CHUNK_PAGE, pageItemParams);
            TreeItem<ChunkTreeItemValue> pageItem = new TreeItem<>(pageValue);
            Node pageIcon = new IconView("/icons/text.png");
            pageItem.setGraphic(pageIcon);
            chunkChild.add(pageItem);
          }
        }
      }
    } catch (Exception e) {
      logger.error("showItemAlignedChunk method catch exception:{}", e);
    }
  }

  /** chunk group tree data set */
  public void chunkGroupTreeDataInit() {
    // 1. 阻塞文件加载完成展示
    while (true) {
      if (tsFileAnalyserV13.getRateOfProcess() > 0.9999999999) {
        break;
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    long dataRegionStartTime = System.currentTimeMillis();

    // 2. cached tsfileName
    loadedTSFileName = tsfileItem.getValue().toString();
    // 3. init TreeView
    //    List<ChunkGroupMetadataModel> chunkGroupMetadataModelList =
    //        this.tsFileAnalyserV13.getChunkGroupMetadataModelList();
    //    if (chunkGroupMetadataModelList == null) {
    //      return;
    //    }

    List<ChunkGroupInfo> chunkGroupInfoList = tsFileAnalyserV13.getChunkGroupInfoList();

    for (ChunkGroupInfo chunkGroupInfo : chunkGroupInfoList) {
      ChunkTreeItemValue chunkGroupMetaItemValue =
          new ChunkTreeItemValue(
              chunkGroupInfo.getDeviceName(),
              TREE_ITEM_TYPE_CHUNK_GROUP,
              chunkGroupInfo.getOffset());
      TreeItem<ChunkTreeItemValue> chunkGroupMetaItem = new TreeItem<>(chunkGroupMetaItemValue);
      Node entityIcon = new IconView("icons/stack.png");
      chunkGroupMetaItem.setGraphic(entityIcon);
      tsfileItem.getChildren().add(chunkGroupMetaItem);
      // 添加检索信息
      timeseriesList.add(chunkGroupMetaItemValue.getName());
      indexMap.put(chunkGroupInfo.getDeviceName(), chunkGroupMetaItem);
    }

    // chunkGroup 下面的各个 chunk 加载
    //    chunkGroupMetadataModelList.forEach(
    //      chunkGroupMetadataModel -> {
    //        ChunkTreeItemValue chunkGroupMetaItemValue =
    //          new ChunkTreeItemValue(
    //          chunkGroupMetadataModel.getDevice(),
    //          TREE_ITEM_TYPE_CHUNK_GROUP,
    //          chunkGroupMetadataModel);
    //        TreeItem<ChunkTreeItemValue> chunkGroupMetaItem = new
    // TreeItem<>(chunkGroupMetaItemValue);
    //        Node entityIcon = new IconView("icons/stack.png");
    //        chunkGroupMetaItem.setGraphic(entityIcon);
    //        tsfileItem.getChildren().add(chunkGroupMetaItem);
    //        // 添加检索信息
    //        timeseriesList.add(chunkGroupMetaItemValue.getName());
    //        indexMap.put(chunkGroupMetadataModel.getDevice(), chunkGroupMetaItem);
    //      });

    tsfileItem.setExpanded(true);
    tsfileLoadStage.close();

    long dataRegionEndTime = System.currentTimeMillis();
    System.out.println("data Region Total time cost:" + (dataRegionEndTime - dataRegionStartTime));

    indexDataInit();
  }

  public void clearParsePageCache() {
    // 1. 清空搜索数据
    if (indexMap != null) {
      indexMap.clear();
    }
    if (timeseriesList != null) {
      timeseriesList.clear();
    }
    // 2. 清空 UI
    if (preTSFileItem != null) {
      // TODO be null ??
      preTSFileItem.getChildren().clear();
      preTSFileItem = null;
    }
    if (indexGroup != null) {
      indexGroup.getChildren().clear();
    }
    // 3. 清空 stage 对象
    tsfileInfoPage = null;
    chunkInfoPage = null;
    pageInfoPage = null;
    measurementSearchPage = null;
    // 4. 清空 others
    selectedFolder = null;
    loadedTSFileName = null;
    // 5. 清空 analyzer 对象
    tsFileAnalyserV13 = null;
  }

  @Override
  public String getName() {
    return "TsFileV0.13";
  }

  public String getLoadedTSFileName() {
    return loadedTSFileName;
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
    public IChunkMetadata iChunkMetadata;
    public ChunkHeader chunkHeader;

    public ChunkWrap(IChunkMetadata iChunkMetadata, ChunkHeader chunkHeader) {
      this.iChunkMetadata = iChunkMetadata;
      this.chunkHeader = chunkHeader;
    }

    public IChunkMetadata getiChunkMetadata() {
      return iChunkMetadata;
    }

    public void setChunkMetadata(IChunkMetadata iChunkMetadata) {
      this.iChunkMetadata = iChunkMetadata;
    }

    public ChunkHeader getChunkHeader() {
      return chunkHeader;
    }

    public void setChunkHeader(ChunkHeader chunkHeader) {
      this.chunkHeader = chunkHeader;
    }
  }

  public static class AlignedChunkWrap {
    private List<IChunkMetadata> chunkMetadataList;
    private List<ChunkHeader> chunkHeaderList;

    public AlignedChunkWrap() {}

    public AlignedChunkWrap(
        List<IChunkMetadata> chunkMetadataList, List<ChunkHeader> chunkHeaderList) {
      this.chunkMetadataList = chunkMetadataList;
      this.chunkHeaderList = chunkHeaderList;
    }

    public List<IChunkMetadata> getChunkMetadataList() {
      return chunkMetadataList;
    }

    public void setChunkMetadataList(List<IChunkMetadata> chunkMetadataList) {
      this.chunkMetadataList = chunkMetadataList;
    }

    public List<ChunkHeader> getChunkHeaderList() {
      return chunkHeaderList;
    }

    public void setChunkHeaderList(List<ChunkHeader> chunkHeaderList) {
      this.chunkHeaderList = chunkHeaderList;
    }
  }

  public static class AlignedPageItemParams {
    private IPageInfo alignedPageInfo;
    private List<ChunkHeader> chunkHeaderList;

    public AlignedPageItemParams() {}

    public AlignedPageItemParams(IPageInfo pageInfo, List<ChunkHeader> chunkHeaderList) {
      this.alignedPageInfo = pageInfo;
      this.chunkHeaderList = chunkHeaderList;
    }

    public IPageInfo getPageInfoList() {
      return alignedPageInfo;
    }

    public void setPageInfoList(IPageInfo pageInfo) {
      this.alignedPageInfo = pageInfo;
    }

    public List<ChunkHeader> getChunkHeaderList() {
      return chunkHeaderList;
    }

    public void setChunkHeaderList(List<ChunkHeader> chunkHeaderList) {
      this.chunkHeaderList = chunkHeaderList;
    }
  }
}
