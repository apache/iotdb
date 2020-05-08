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

import java.awt.event.ActionEvent;
import java.io.IOException;
import javax.swing.AbstractAction;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import org.apache.iotdb.db.tools.logvisual.LogVisualizer;

/**
 * LoadLogBox contains a JButton to load a log parser into memory to prepare for visualization
 * and a JLabel to show whether the loading of the log parser is successful.
 */
class LoadLogBox extends Box{

  private JLabel status;
  private JButton loadLogButton;
  private LogVisualizer visualizer;

  LoadLogBox(LogVisualizer visualizer) {
    super(BoxLayout.Y_AXIS);
    this.visualizer = visualizer;

    status = new JLabel("No logs are loaded");
    loadLogButton = new JButton("Load logs");
    loadLogButton.addActionListener(this::onLoadLog);

    add(status);
    add(loadLogButton);
  }

  private void onLoadLog(ActionEvent e) {
    try {
      visualizer.loadLogParser();
      status.setText("Logs are successfully loaded");
    } catch (IOException e1) {
      JOptionPane.showMessageDialog(LoadLogBox.this, "Cannot load logs: " + e1);
    }
  }
}