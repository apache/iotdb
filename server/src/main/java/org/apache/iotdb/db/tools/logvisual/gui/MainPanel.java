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

package org.apache.iotdb.db.tools.logvisual.gui;

import java.io.File;
import java.util.Properties;
import javax.swing.JPanel;
import org.apache.iotdb.db.tools.logvisual.LogVisualizer;
import org.apache.iotdb.db.tools.logvisual.conf.PropertyKeys;
import org.apache.iotdb.db.tools.logvisual.gui.LogVisualizationGui.PropertyChangeCallback;
import org.apache.iotdb.db.tools.logvisual.gui.PlanBox.ExecutePlanCallback;

/**
 * MainPanel provides components that enable the user to choose log file to be visualized,
 * manipulate or execute visualization plans.
 */
class MainPanel extends JPanel {

  /**
   * to select a log file that will be visualized.
   */
  private FileSelectionBox logFileSelectionBox;
  /**
   * to select a file that describe how to parse the logs.
   */
  private FileSelectionBox parserPropertyBox;
  /**
   * to generate a log parser and prepare to load the logs.
   */
  private LoadLogBox loadLogBox;
  /**
   * to provide means of manipulating the visualizations plans and execute them.
   */
  private PlanBox planBox;

  /**
   * A backend that actually performs the visualization.
   */
  private LogVisualizer visualizer;

  /**
   * When a user choose a new file, call this to remember the choice.
   */
  private PropertyChangeCallback propertyChangeCallback;

  MainPanel(LogVisualizer logVisualizer, ExecutePlanCallback executePlanCallback,
      Properties properties, PropertyChangeCallback propertyChangeCallback) {
    this.visualizer = logVisualizer;
    this.propertyChangeCallback = propertyChangeCallback;

    setLayout(null);

    logFileSelectionBox = new FileSelectionBox("LogFilePath", this::onLogFileSelected,
        properties.getProperty(PropertyKeys.DEFAULT_LOG_FILE_PATH.getKey()));
    logFileSelectionBox.setLocation(0, 0);
    logFileSelectionBox.setSize(380, 40);

    parserPropertyBox = new FileSelectionBox("ParserPropertyFilePath",
        this::onParserPropertySelected, properties.getProperty(PropertyKeys
        .DEFAULT_PARSER_FILE_PATH.getKey()));
    parserPropertyBox.setLocation(0, 45);
    parserPropertyBox.setSize(380, 40);

    loadLogBox = new LoadLogBox(logVisualizer);
    loadLogBox.setLocation(450, 0);
    loadLogBox.setSize(300, 50);

    planBox = new PlanBox(logVisualizer, executePlanCallback, properties.getProperty(PropertyKeys
        .DEFAULT_PLAN_PATH.getKey()), propertyChangeCallback);
    planBox.setLocation(0, 100);
    planBox.setSize(750, 430);

    add(logFileSelectionBox);
    add(parserPropertyBox);
    add(loadLogBox);
    add(planBox);
  }

  private void onLogFileSelected(File logFile) {
    visualizer.setLogFile(logFile);
    propertyChangeCallback.call(PropertyKeys.DEFAULT_LOG_FILE_PATH.getKey(), logFile.getPath());
  }

  private void onParserPropertySelected(File parserPropertyFile) {
    visualizer.setParserPropertyFile(parserPropertyFile);
    propertyChangeCallback.call(PropertyKeys.DEFAULT_PARSER_FILE_PATH.getKey(), parserPropertyFile
        .getPath());
  }

}