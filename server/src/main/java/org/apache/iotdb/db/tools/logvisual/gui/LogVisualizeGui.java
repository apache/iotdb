/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.tools.logvisual.gui;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JTabbedPane;
import javax.swing.WindowConstants;
import org.apache.iotdb.db.tools.logvisual.LogVisualizer;
import org.apache.iotdb.db.tools.logvisual.TimeSeriesStatistics;
import org.jfree.chart.JFreeChart;

public class LogVisualizeGui {

  private final String DEFAULT_PROPERTY = "visual.config";
  private final int DEFAULT_HEIGHT = 600;
  private final int DEFAULT_WIDTH = 800;

  private LogVisualizer visualizer;
  private JFrame mainFrame;
  private JTabbedPane tabbedPane;
  private MainPanel mainPanel;

  private Map<String, ResultPlotTab> resultPlotPanels = new HashMap<>();
  private Map<String, ResultStatisticTab> resultTablePanels = new HashMap<>();
  private String propertyFilePath;
  private Properties properties;

  public LogVisualizeGui(String propertyFilePath) throws IOException {
    properties = new Properties();
    if (propertyFilePath == null) {
      propertyFilePath = DEFAULT_PROPERTY;
    }
    this.propertyFilePath = propertyFilePath;
    File propertyFile = new File(propertyFilePath);
    if (propertyFile.exists()) {
      try (FileInputStream fileInputStream = new FileInputStream(propertyFilePath);
      BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)){
        properties.load(bufferedInputStream);
      }
    }

    this.visualizer = new LogVisualizer();
    JFrame.setDefaultLookAndFeelDecorated(true);
    mainFrame = new JFrame("Log Visualizer");
    mainFrame.setSize(DEFAULT_WIDTH, DEFAULT_HEIGHT);
    mainFrame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
    mainFrame.setResizable(false);

    tabbedPane = new JTabbedPane();
    mainPanel = new MainPanel(visualizer, this::onPlanExecuted, properties, this::onPropertyChange);
    tabbedPane.add(mainPanel, "Main");

    mainFrame.add(tabbedPane);

    mainFrame.setVisible(true);
  }

  private void onPlanExecuted(String planName, Map<String, JFreeChart> charts, Map<String,
      List<TimeSeriesStatistics>> statisticMap) {
    String tabName = planName + "-plot";
    ResultPlotTab resultPlotTab = new ResultPlotTab(tabName, charts, this::onTabClose);
    ResultPlotTab oldPlotTab = resultPlotPanels.get(tabName);
    if (oldPlotTab != null) {
      tabbedPane.remove(oldPlotTab);
    }
    resultPlotPanels.put(tabName, resultPlotTab);
    tabbedPane.add(resultPlotTab);

    tabName = planName + "-statistics";
    ResultStatisticTab resultStatisticTab = new ResultStatisticTab(tabName, statisticMap,
        this::onTabClose);
    ResultStatisticTab oldTableTab = resultTablePanels.get(tabName);
    if (oldTableTab != null) {
      tabbedPane.remove(oldPlotTab);
    }
    resultTablePanels.put(tabName, resultStatisticTab);
    tabbedPane.add(resultStatisticTab);
  }

  private void onPropertyChange(String key, String value) {
    properties.put(key, value);
    try (FileWriter writer = new FileWriter(propertyFilePath);
        BufferedWriter bufferedWriter = new BufferedWriter(writer)) {
      properties.store(bufferedWriter,"");
    } catch (IOException e) {
      JOptionPane.showMessageDialog(mainPanel, "Cannot save property files: ");
    }
  }

  public interface PropertyChangeCallback {
    void call(String key, String value);
  }

  private void onTabClose(String tabName) {
    ClosableTab tab = resultTablePanels.remove(tabName);
    if (tab != null) {
      tabbedPane.remove(tab);
      return;
    }
    tab = resultPlotPanels.remove(tabName);
    if (tab != null) {
      tabbedPane.remove(tab);
    }
  }

  public static void main(String[] args) throws IOException {
    String propertyFilePath = null;
    if (args.length > 0) {
      propertyFilePath = args[0];
    }
    LogVisualizeGui gui = new LogVisualizeGui(propertyFilePath);
  }


}