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

import java.text.SimpleDateFormat;
import java.util.regex.Pattern;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.JOptionPane;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import org.apache.iotdb.db.tools.logvisual.LogEntry.LogLevel;
import org.apache.iotdb.db.tools.logvisual.VisualUtils;
import org.apache.iotdb.db.tools.logvisual.VisualizationPlan;

/**
 * PlanDetailPanel displays the information of a visualization plan.
 */
public class PlanDetailPanel extends JScrollPane {

  private VisualizationPlan plan;

  private JTextField nameField = new JTextField();
  private JTextField patternField = new JTextField();
  private JTextField measurementsField = new JTextField();
  private JTextField legendsField = new JTextField();
  private JTextField tagField = new JTextField();
  private JTextField levelField = new JTextField();
  private JTextField threadNameField = new JTextField();
  private JTextField classNameField = new JTextField();
  private JTextField lineNumField = new JTextField();
  private JTextField datePatternField = new JTextField();
  private JTextField startDateField = new JTextField();
  private JTextField endDateField = new JTextField();

  PlanDetailPanel() {
    super(null, JScrollPane.VERTICAL_SCROLLBAR_ALWAYS, JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
    setBorder(BorderFactory.createTitledBorder("Plan detail"));

    nameField.setBorder(BorderFactory.createTitledBorder("Plan name"));
    patternField.setBorder(BorderFactory.createTitledBorder("Content Pattern"));
    measurementsField.setBorder(BorderFactory.createTitledBorder("Measurement positions"));
    legendsField.setBorder(BorderFactory.createTitledBorder("Legends"));
    tagField.setBorder(BorderFactory.createTitledBorder("Tag positions"));
    levelField.setBorder(BorderFactory.createTitledBorder("Log level"));
    threadNameField.setBorder(BorderFactory.createTitledBorder("Allowed thread names"));
    classNameField.setBorder(BorderFactory.createTitledBorder("Allowed class names"));
    lineNumField.setBorder(BorderFactory.createTitledBorder("Allowed line numbers"));
    datePatternField.setBorder(BorderFactory.createTitledBorder("Date pattern"));
    startDateField.setBorder(BorderFactory.createTitledBorder("Start date"));
    endDateField.setBorder(BorderFactory.createTitledBorder("End date"));

    Box box = Box.createVerticalBox();

    box.add(nameField);
    box.add(patternField);
    box.add(measurementsField);
    box.add(legendsField);
    box.add(tagField);
    box.add(levelField);
    box.add(threadNameField);
    box.add(classNameField);
    box.add(lineNumField);
    box.add(datePatternField);
    box.add(startDateField);
    box.add(endDateField);

    setViewportView(box);
  }

  /**
   * Set the currently displayed plan. If it is null, clean the display.
   * @param plan
   */
  public void setPlan(VisualizationPlan plan) {
    this.plan = plan;
    updateFields();
  }

  private void resetFields() {
    nameField.setText("");
    patternField.setText("");
    measurementsField.setText("");
    legendsField.setText("");
    tagField.setText("");
    levelField.setText("");
    threadNameField.setText("");
    classNameField.setText("");
    lineNumField.setText("");
    datePatternField.setText("");
    startDateField.setText("");
    endDateField.setText("");
  }

  private void updateFields() {
    resetFields();
    if (plan == null) {
      return;
    }
    nameField.setText(plan.getName());
    patternField.setText(plan.getContentPattern().pattern());

    if (plan.getMeasurementPositions() != null) {
      measurementsField.setText(VisualUtils.intArrayToString(plan.getMeasurementPositions()));
    }
    if (plan.getLegends() != null) {
      legendsField.setText(String.join(",", plan.getLegends()));
    }
    if (plan.getTagPositions() != null) {
      tagField.setText(VisualUtils.intArrayToString(plan.getTagPositions()));
    }
    levelField.setText(plan.getLogFilter().getMinLevel().name());
    if (plan.getLogFilter().getThreadNameWhiteList() != null) {
      threadNameField.setText(String.join(",", plan.getLogFilter().getThreadNameWhiteList()));
    }
    if (plan.getLogFilter().getClassNameWhiteList() != null) {
      classNameField.setText(String.join(",", plan.getLogFilter().getClassNameWhiteList()));
    }
    if (plan.getLogFilter().getLineNumWhiteList() != null) {
      lineNumField.setText(VisualUtils.intArrayToString(plan.getLogFilter().getLineNumWhiteList()));
    }
    if (plan.getLogFilter().getDatePattern() != null) {
      SimpleDateFormat datePatten = (SimpleDateFormat) plan.getLogFilter().getDatePattern();
      datePatternField.setText(datePatten.toPattern());
      if (plan.getLogFilter().getStartDate() != null) {
        startDateField.setText(datePatten.format(plan.getLogFilter().getStartDate()));
      }
      if (plan.getLogFilter().getEndDate() != null) {
        endDateField.setText(datePatten.format(plan.getLogFilter().getEndDate()));
      }
    }
  }

  /**
   * Update the current displayed plan after it is modified amd save it to a file.
   */
  public void updatePlan() {
    if (plan == null) {
      return;
    }

    String name = nameField.getText();
    String contentPattern = patternField.getText();
    String measurementPositons = measurementsField.getText();
    String legends = legendsField.getText();
    String tagPositions = tagField.getText();
    String logLevel = levelField.getText();
    String allowedThreads = threadNameField.getText();
    String allowedClasses = classNameField.getText();
    String allowedLineNums = lineNumField.getText();
    String datePattern = datePatternField.getText();
    String startDate = startDateField.getText();
    String endDate = endDateField.getText();

    // validate the fields
    if (name.matches("\\s*")) {
      JOptionPane.showMessageDialog(this, "Name cannot be empty");
      return;
    }
    if (contentPattern.matches("\\s*")) {
      JOptionPane.showMessageDialog(this, "Content pattern cannot be empty");
      return;
    }
    if (measurementPositons.matches("\\s*")) {
      measurementPositons = null;
    }
    if (legends.matches("\\s*")) {
      legends = null;
    }
    if (tagPositions.matches("\\s*")) {
      tagPositions = null;
    }
    if (logLevel.matches("\\s*")) {
      logLevel = LogLevel.DEBUG.name();
    }
    if (allowedThreads.matches("\\s*")) {
      allowedThreads = null;
    }
    if (allowedClasses.matches("\\s*")) {
      allowedClasses = null;
    }
    if (allowedLineNums.matches("\\s*")) {
      allowedLineNums = null;
    }
    if (datePattern.matches("\\s*")) {
      datePattern = null;
    }
    if (startDate.matches("\\s*")) {
      startDate = null;
    }
    if (endDate.matches("\\s*")) {
      endDate = null;
    }
    if ((startDate != null || endDate != null) && datePattern == null) {
      JOptionPane.showMessageDialog(this, "Date pattern cannot be empty if either start date or"
          + " end date is not empty");
      return;
    }

    try {
      plan.setName(name);
      plan.setContentPattern(Pattern.compile(contentPattern));
      plan.setMeasurementPositions(measurementPositons != null ? VisualUtils.parseIntArray
          (measurementPositons) : null);
      plan.setLegends(legends != null ? legends.split(",") : null);
      plan.setTagPositions(tagPositions != null ? VisualUtils.parseIntArray(tagPositions) : null);
      plan.getLogFilter().setClassNameWhiteList(allowedClasses != null ? allowedClasses.split(",")
          : null);
      plan.getLogFilter().setThreadNameWhiteList(allowedThreads != null ? allowedThreads.split(","
          + "") : null);
      plan.getLogFilter().setLineNumWhiteList(allowedLineNums != null ? VisualUtils.parseIntArray
          (allowedLineNums) : null);
      plan.getLogFilter().setMinLevel(LogLevel.valueOf(logLevel));
      SimpleDateFormat simpleDateFormat = datePattern != null ? new SimpleDateFormat(datePattern) :
          null;
      plan.getLogFilter().setDatePattern(simpleDateFormat);
      plan.getLogFilter().setStartDate(startDate != null ? simpleDateFormat.parse(startDate) : null);
      plan.getLogFilter().setEndDate(endDate != null ? simpleDateFormat.parse(endDate) : null);

      plan.saveAsFile();
    } catch (Exception e) {
      JOptionPane.showMessageDialog(this, e.getMessage());
    }
  }
}