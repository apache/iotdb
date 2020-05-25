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

import java.awt.GridBagLayout;
import java.util.Map;
import javax.swing.BorderFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;

/**
 * ResultPlotTab shows the timeseries-plot of the log events.
 */
class ResultPlotTab extends ClosableComboTab {

  private ChartPanel chartPanel;

  ResultPlotTab(String planName, Map<String, JFreeChart> charts, TabCloseCallBack closeCallBack) {
    super(planName, charts, closeCallBack);

    chartPanel = new ChartPanel(null);
    chartPanel.setBorder(BorderFactory.createTitledBorder("Plot area"));
    chartPanel.setLayout(new GridBagLayout());
    chartPanel.setLocation(0, 60);
    chartPanel.setSize(800, 480);
    add(chartPanel);
  }

  void onItemSelected(Object chart){
    chartPanel.setChart((JFreeChart) chart);
  }
}