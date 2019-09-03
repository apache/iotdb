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
import java.awt.event.ActionEvent;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import org.apache.iotdb.db.tools.logvisual.LogVisualizer;
import org.apache.iotdb.db.tools.logvisual.TimeSeriesStatistics;
import org.apache.iotdb.db.tools.logvisual.VisualizationPlan;
import org.apache.iotdb.db.tools.logvisual.conf.PropertyKeys;
import org.apache.iotdb.db.tools.logvisual.exceptions.VisualizationException;
import org.apache.iotdb.db.tools.logvisual.gui.LogVisualizationGui.PropertyChangeCallback;
import org.jfree.chart.JFreeChart;

/**
 * PlanBox provides interfaces to save, load, create, delete and execute visualization plans.
 */
@SuppressWarnings("unused") // ignore the event parameter
class PlanBox extends Box{
  private JLabel panelName;
  private JButton loadPlanButton;
  private JButton executePlanButton;
  private JButton savePlanButton;
  private JButton createPlanButton;
  private JButton deletePlanButton;
  private JButton exportResultButton;

  // display of plans
  private JScrollPane scrollPane;
  private DefaultListModel<VisualizationPlan> planListModel;
  private JList planList;
  private PlanDetailPanel planDetailPanel;

  // plan execution backend
  private LogVisualizer visualizer;

  // call this to create tabs to display the results when a plan is executed
  private ExecutePlanCallback executePlanCallback;
  // call this to remember the choice when the user has loaded new plans
  private PropertyChangeCallback propertyChangeCallback;

  PlanBox(LogVisualizer visualizer, ExecutePlanCallback executePlanCallback, String defaultPlanPath,
      PropertyChangeCallback propertyChangeCallback) {
    super(BoxLayout.X_AXIS);

    this.visualizer = visualizer;
    this.executePlanCallback = executePlanCallback;
    this.propertyChangeCallback = propertyChangeCallback;

    panelName = new JLabel("Visualization plans");
    loadPlanButton = new JButton("Load plan");
    executePlanButton = new JButton("Execute plan");
    savePlanButton = new JButton("Save plan");
    createPlanButton = new JButton("Create plan");
    deletePlanButton = new JButton("Delete plan");
    exportResultButton = new JButton("Export result");
    panelName.setAlignmentX(CENTER_ALIGNMENT);
    loadPlanButton.setAlignmentX(CENTER_ALIGNMENT);
    executePlanButton.setAlignmentX(CENTER_ALIGNMENT);
    savePlanButton.setAlignmentX(CENTER_ALIGNMENT);
    createPlanButton.setAlignmentX(CENTER_ALIGNMENT);
    deletePlanButton.setAlignmentX(CENTER_ALIGNMENT);
    exportResultButton.setAlignmentX(CENTER_ALIGNMENT);

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
    vBox.add(savePlanButton);
    vBox.add(Box.createVerticalStrut(5));
    vBox.add(createPlanButton);
    vBox.add(Box.createVerticalStrut(5));
    vBox.add(deletePlanButton);
    vBox.add(Box.createVerticalStrut(5));
    vBox.add(executePlanButton);
    vBox.add(Box.createVerticalStrut(5));
    vBox.add(exportResultButton);
    vBox.add(Box.createGlue());
    add(vBox);

    planDetailPanel = new PlanDetailPanel();
    planDetailPanel.setPreferredSize(new Dimension(400, 300));
    add(planDetailPanel);

    planList.addListSelectionListener(this::onPlanSelectionChanged);
    loadPlanButton.addActionListener(this::onLoadPlan);
    executePlanButton.addActionListener(this::onExecutePlan);
    savePlanButton.addActionListener(this::onPlanSave);
    createPlanButton.addActionListener(this::onCreatePlan);
    deletePlanButton.addActionListener(this::onDeletePlan);
    exportResultButton.addActionListener(this::onExportResult);

    exportResultButton.setEnabled(false);

    if (defaultPlanPath != null) {
      // load default plans if given
      String[] defaultPaths = defaultPlanPath.split(";");
      File[] defaultPlanFiles = new File[defaultPaths.length];
      for (int i = 0; i < defaultPaths.length; i++) {
        defaultPlanFiles[i] = new File(defaultPaths[i]);
      }
      try {
        // read the plans from the disk
        visualizer.loadPlans(defaultPlanFiles);
        Collection<VisualizationPlan> planList = visualizer.listPlans();
        // show plans in the gui
        updatePlan(planList);
      } catch (IOException e1) {
        JOptionPane.showMessageDialog(this, "Cannot load plan: " + e1);
      }
    }
  }

  private void onLoadPlan(ActionEvent e) {
    JFileChooser fileChooser = new JFileChooser();
    fileChooser.setMultiSelectionEnabled(true);
    fileChooser.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
    // let the user choose plan files or directories that contain visualization plans
    int status = fileChooser.showOpenDialog(this);
    if (status == JFileChooser.APPROVE_OPTION) {
      File[] chosenFiles = fileChooser.getSelectedFiles();
      try {
        // read plans from disk
        visualizer.loadPlans(chosenFiles);
        Collection<VisualizationPlan> planList = visualizer.listPlans();
        // display the plans in the panel
        updatePlan(planList);

        // save the paths so that the tool can load the plans automatically next time
        if (chosenFiles.length > 0) {
          StringBuilder builder = new StringBuilder(chosenFiles[0].getPath());
          for (int i = 1; i < chosenFiles.length; i++) {
            builder.append(";").append(chosenFiles[i].getPath());
          }
          propertyChangeCallback.call(PropertyKeys.DEFAULT_PLAN_PATH.getKey(), builder.toString());
        }
      } catch (IOException e1) {
        JOptionPane.showMessageDialog(this, "Cannot load plan: " + e1.getMessage());
      }
    }
  }

  private void updatePlan(Collection<VisualizationPlan> plans) {
    planListModel.clear();
    for (VisualizationPlan plan : plans) {
      planListModel.addElement(plan);
    }
  }

  private void onExecutePlan(ActionEvent e) {
    VisualizationPlan plan = (VisualizationPlan) planList.getSelectedValue();
    if (plan == null) {
      return;
    }
    try {
      visualizer.executePlan(plan);
    } catch (VisualizationException e1) {
      JOptionPane.showMessageDialog(this, "Cannot execute plan: " + e1.getMessage());
      return;
    }
    // timeseries plots of each measurement in the visualization plan
    Map<String, JFreeChart> charts = visualizer.getCharts();
    // statistics (count, mean, max, min) of each measurement
    Map<String, List<TimeSeriesStatistics>> statisticMap = visualizer.getStatisticsMap();
    executePlanCallback.call(plan.getName(), charts, statisticMap);
    exportResultButton.setEnabled(true);
    JOptionPane.showMessageDialog(this,
        String.format("Plan is successfully executed, found %d log groups", charts.size()));
  }

  private void onPlanSelectionChanged(ListSelectionEvent e) {
    VisualizationPlan plan = (VisualizationPlan) planList.getSelectedValue();
    if (plan == null) {
      return;
    }
    // update the display of the panel according to the new plan
    planDetailPanel.setPlan(plan);
  }

  private void onPlanSave(ActionEvent e) {
    // update the content of the plan according to the text fields
    planDetailPanel.updatePlan();
  }

  private void onCreatePlan(ActionEvent e) {
    JFileChooser fileChooser = new JFileChooser();
    // let the user to choose a place for the new plan
    int status = fileChooser.showOpenDialog(this);
    if (status == JFileChooser.APPROVE_OPTION) {
      File chosenFile = fileChooser.getSelectedFile();
      VisualizationPlan plan = new VisualizationPlan();
      plan.setPlanFilePath(chosenFile.getPath());
      // the name of the file will also be the name of the plan
      plan.setName(chosenFile.getName());
      // a default plan matches every thing
      plan.setContentPattern(Pattern.compile(".*"));

      planListModel.addElement(plan);
      // the new plan will be focused on
      planList.setSelectedIndex(planListModel.getSize() - 1);
      planDetailPanel.setPlan(plan);
    }
  }

  private void onDeletePlan(ActionEvent e) {
    VisualizationPlan plan = (VisualizationPlan) planList.getSelectedValue();
    if (plan == null) {
      return;
    }

    int status = JOptionPane.showConfirmDialog(this, "Do you really want to delete this plan? "
        + "(Cannot be undone)", "confirm", JOptionPane.YES_NO_OPTION);
    if (status == JOptionPane.YES_OPTION) {
      File file = new File(plan.getPlanFilePath());
      file.delete();
      planListModel.removeElement(plan);
      // update the display since the deleted one is always the one being displayed
      planDetailPanel.setPlan(null);
    }
  }

  private void onExportResult(ActionEvent e) {
    JFileChooser fileChooser = new JFileChooser();
    fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
    // let the user choose plan files or directories that contain visualization plans
    int status = fileChooser.showOpenDialog(this);
    if (status == JFileChooser.APPROVE_OPTION) {
      File distDir = fileChooser.getSelectedFile();
      try {
        visualizer.saveResults(distDir.getPath());
        JOptionPane.showMessageDialog(this, "Export successfully");
      } catch (VisualizationException e1) {
        JOptionPane.showMessageDialog(this,"Cannot export results:" + e1.getMessage());
      }
    }
  }

  public interface ExecutePlanCallback {
    // call this to create new tabs to show the results after the plan is executed
    void call(String planName, Map<String, JFreeChart> charts, Map<String,
        List<TimeSeriesStatistics>> statisticMap);
  }

}