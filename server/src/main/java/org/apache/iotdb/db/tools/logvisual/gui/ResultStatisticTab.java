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

import java.awt.BorderLayout;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;
import javax.swing.table.TableRowSorter;
import org.apache.iotdb.db.tools.logvisual.TimeSeriesStatistics;

class ResultStatisticTab extends ClosableTab {

  private TableModel tableModel;
  private JTable table;

  ResultStatisticTab(String planName, Map<String, List<TimeSeriesStatistics>>
      timeseriesStatistics, TabCloseCallBack closeCallBack) {
    super(planName, closeCallBack);

    table = new JTable();

    Box box = Box.createVerticalBox();
    box.add(table.getTableHeader());
    box.add(table);
    JScrollPane scrollPane = new JScrollPane(box);
    scrollPane.setLocation(0, 100);
    scrollPane.setSize(800, 600);
    add(scrollPane);

    Object[] header = TimeSeriesStatistics.HEADER;
    List<TimeSeriesStatistics> allStatistics = new ArrayList<>();
    for (List<TimeSeriesStatistics> seriesStatistics : timeseriesStatistics.values()) {
      allStatistics.addAll(seriesStatistics);
    }
    allStatistics.sort(Comparator.comparing(TimeSeriesStatistics::getName));
    Object[][] data = new Object[allStatistics.size()][];
    for (int i = 0; i < allStatistics.size(); i++) {
      data[i] = allStatistics.get(i).toArray();
    }
    tableModel = new DefaultTableModel(data, header);
    table.setModel(tableModel);
    table.setRowSorter(new TableRowSorter<>(tableModel));
  }
}