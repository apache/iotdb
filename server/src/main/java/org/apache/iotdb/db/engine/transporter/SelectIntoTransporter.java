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

package org.apache.iotdb.db.engine.transporter;

import org.apache.iotdb.db.exception.IoTDBException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertMultiTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SelectIntoTransporter {

  private static final Pattern leveledPathNodePattern = Pattern.compile("\\$\\{\\w+}");

  private final QueryDataSet queryDataSet;
  private final List<PartialPath> intoPaths;
  private final int fetchSize;

  private InsertTabletPlanGenerator[] insertTabletPlanGenerators;

  public SelectIntoTransporter(
      QueryDataSet queryDataSet, List<PartialPath> intoPaths, int fetchSize) {
    this.queryDataSet = queryDataSet;
    this.intoPaths = intoPaths;
    this.fetchSize = fetchSize;
  }

  public void transport() throws IoTDBException, IOException {
    generateActualIntoPaths();
    constructTabletGenerators();
    doTransport();
  }

  private void generateActualIntoPaths() throws IllegalPathException {
    for (int i = 0; i < intoPaths.size(); ++i) {
      intoPaths.set(i, generateActualIntoPath(i));
    }
  }

  private PartialPath generateActualIntoPath(int index) throws IllegalPathException {
    String[] nodes = intoPaths.get(index).getNodes();

    int indexOfLeftBracket = nodes[0].indexOf("(");
    if (indexOfLeftBracket != -1) {
      nodes[0] = nodes[0].substring(indexOfLeftBracket + 1);
    }
    int indexOfRightBracket = nodes[nodes.length - 1].indexOf(")");
    if (indexOfRightBracket != -1) {
      nodes[nodes.length - 1] = nodes[nodes.length - 1].substring(0, indexOfRightBracket);
    }

    StringBuffer sb = new StringBuffer();
    Matcher m = leveledPathNodePattern.matcher(queryDataSet.getPaths().get(index).getFullPath());
    while (m.find()) {
      String param = m.group();
      String value = nodes[Integer.parseInt(param.substring(2, param.length() - 1).trim())];
      m.appendReplacement(sb, value == null ? "" : value);
    }
    m.appendTail(sb);
    return new PartialPath(sb.toString());
  }

  private void constructTabletGenerators() {
    Map<String, InsertTabletPlanGenerator> deviceTabletGeneratorMap = new HashMap<>();
    for (int i = 0, intoPathsSize = intoPaths.size(); i < intoPathsSize; i++) {
      String device = intoPaths.get(i).getDevice();
      if (!deviceTabletGeneratorMap.containsKey(device)) {
        deviceTabletGeneratorMap.put(
            device, new InsertTabletPlanGenerator(queryDataSet, intoPaths, device));
      }
      deviceTabletGeneratorMap.get(device).addMeasurementIdIndex(i);
    }
    insertTabletPlanGenerators =
        deviceTabletGeneratorMap.values().toArray(new InsertTabletPlanGenerator[0]);
  }

  private void doTransport() throws IOException {
    while (queryDataSet.hasNext()) {
      List<InsertTabletPlan> insertTabletPlanList = new ArrayList<>();
      for (InsertTabletPlanGenerator insertTabletPlanGenerator : insertTabletPlanGenerators) {
        insertTabletPlanGenerator.constructNewTablet();
      }
      collectRowRecordIntoInsertTabletPlanGenerators();
      for (InsertTabletPlanGenerator insertTabletPlanGenerator : insertTabletPlanGenerators) {
        insertTabletPlanList.add(insertTabletPlanGenerator.getInsertTabletPlan());
      }
      InsertMultiTabletPlan insertMultiTabletPlan = new InsertMultiTabletPlan(insertTabletPlanList);
      // TODO: execute insertMultiTabletPlan
    }
  }

  private void collectRowRecordIntoInsertTabletPlanGenerators() throws IOException {
    int count = 0;
    while (queryDataSet.hasNext() && count < fetchSize) {
      RowRecord rowRecord = queryDataSet.next();
      for (InsertTabletPlanGenerator insertTabletPlanGenerator : insertTabletPlanGenerators) {
        insertTabletPlanGenerator.collectRowRecord(rowRecord);
      }
      ++count;
    }
  }
}
