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

import java.awt.Dimension;
import java.awt.ScrollPane;
import java.awt.event.ActionEvent;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import org.apache.iotdb.db.tools.logvisual.LogVisualizer;
import org.apache.iotdb.db.tools.logvisual.TimeSeriesStatistics;
import org.apache.iotdb.db.tools.logvisual.VisualizationPlan;
import org.apache.iotdb.db.tools.logvisual.conf.PropertyKeys;
import org.apache.iotdb.db.tools.logvisual.exceptions.AnalyzeException;
import org.apache.iotdb.db.tools.logvisual.gui.LogVisualizeGui.PropertyChangeCallback;
import org.jfree.chart.JFreeChart;

public class PlanBox extends Box{
  private JLabel panelName;
  private JButton loadPlanButton;
  private JButton executePlanButton;
  private JButton savePlanButtion;
  private JScrollPane scrollPane;
  private DefaultListModel<VisualizationPlan> planListModel;
  private JList planList;
  private PlanDetailPanel planDetailPanel;

  private LogVisualizer visualizer;

  private ExecutePlanCallback executePlanCallback;
  private PropertyChangeCallback propertyChangeCallback;

  public PlanBox(LogVisualizer visualizer, ExecutePlanCallback executePlanCallback, String defaultPlanPath,
      PropertyChangeCallback propertyChangeCallback) {
    super(BoxLayout.X_AXIS);

    this.visualizer = visualizer;
    this.executePlanCallback = executePlanCallback;
    this.propertyChangeCallback = propertyChangeCallback;

    panelName = new JLabel("Visualization plans");
    loadPlanButton = new JButton("Load plan");
    executePlanButton = new JButton("Execute plan");
    savePlanButtion = new JButton("Save plan");
    panelName.setAlignmentX(CENTER_ALIGNMENT);
    loadPlanButton.setAlignmentX(CENTER_ALIGNMENT);
    executePlanButton.setAlignmentX(CENTER_ALIGNMENT);
    savePlanButtion.setAlignmentX(CENTER_ALIGNMENT);

    planListModel = new DefaultListModel<>();
    planList = new JList<>(planListModel);
    planList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
    scrollPane = new JScrollPane(planList);
    add(scrollPane);
    add(Box.createHorizontalStrut(10));

    Box vBox = Box.createVerticalBox();
    vBox.add(panelName);
    vBox.add(loadPlanButton);
    vBox.add(Box.createVerticalStrut(5));
    vBox.add(executePlanButton);
    vBox.add(Box.createVerticalStrut(5));
    vBox.add(savePlanButtion);
    vBox.add(Box.createGlue());
    add(vBox);
    setAlignmentY(0.5f);

    planDetailPanel = new PlanDetailPanel();
    planDetailPanel.setPreferredSize(new Dimension(400, 300));
    add(planDetailPanel);

    planList.addListSelectionListener(this::onPlanSelectionChanged);
    loadPlanButton.addActionListener(this::onLoadPlanButtonClick);
    executePlanButton.addActionListener(this::onExecutePlanButtonClick);
    savePlanButtion.addActionListener(this::onPlanSave);

    if (defaultPlanPath != null) {
      String[] defaultPaths = defaultPlanPath.split(";");
      File[] defaultPlanFiles = new File[defaultPaths.length];
      for (int i = 0; i < defaultPaths.length; i++) {
        defaultPlanFiles[i] = new File(defaultPaths[i]);
      }
      try {
        visualizer.loadPlans(defaultPlanFiles);
        Collection<VisualizationPlan> planList = visualizer.listPlans();
        updatePlan(planList);
      } catch (IOException e1) {
        JOptionPane.showMessageDialog(this, "Cannot load plan: " + e1);
      }
    }
  }

  private void onLoadPlanButtonClick(ActionEvent e) {
    JFileChooser fileChooser = new JFileChooser();
    fileChooser.setMultiSelectionEnabled(true);
    fileChooser.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
    int status = fileChooser.showOpenDialog(this);
    if (status == JFileChooser.APPROVE_OPTION) {
      File[] chosenFiles = fileChooser.getSelectedFiles();
      try {
        visualizer.loadPlans(chosenFiles);
        Collection<VisualizationPlan> planList = visualizer.listPlans();
        updatePlan(planList);

        if (chosenFiles.length > 0) {
          StringBuilder builder = new StringBuilder(chosenFiles[0].getPath());
          for (int i = 1; i < chosenFiles.length; i++) {
            builder.append(";").append(chosenFiles[i].getPath());
          }
          propertyChangeCallback.call(PropertyKeys.DEFAULT_PLAN_PATH.getKey(), builder.toString());
        }
      } catch (IOException e1) {
        JOptionPane.showMessageDialog(this, "Cannot load plan: " + e1);
      }
    }
  }

  private void updatePlan(Collection<VisualizationPlan> plans) {
    planListModel.clear();
    for (VisualizationPlan plan : plans) {
      planListModel.addElement(plan);
    }
  }

  private void onExecutePlanButtonClick(ActionEvent e) {
    VisualizationPlan plan = (VisualizationPlan) planList.getSelectedValue();
    if (plan == null) {
      return;
    }
    try {
      visualizer.executePlan(plan);
    } catch (AnalyzeException e1) {
      JOptionPane.showMessageDialog(this, "Cannot execute plan: " + e1.getMessage());
    }
    Map<String, JFreeChart> charts = visualizer.getCharts();
    Map<String, List<TimeSeriesStatistics>> statisticMap = visualizer.getStatisticsMap();
    executePlanCallback.call(plan.getName(), charts, statisticMap);
  }

  private void onPlanSelectionChanged(ListSelectionEvent e) {
    VisualizationPlan plan = (VisualizationPlan) planList.getSelectedValue();
    if (plan == null) {
      return;
    }
    planDetailPanel.setPlan(plan);
  }

  private void onPlanSave(ActionEvent e) {
    planDetailPanel.updatePlan();
  }

  public interface ExecutePlanCallback {
    void call(String planName, Map<String, JFreeChart> charts, Map<String,
        List<TimeSeriesStatistics>> statisticMap);
  }

}