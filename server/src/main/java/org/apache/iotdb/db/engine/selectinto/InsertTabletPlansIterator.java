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

package org.apache.iotdb.db.engine.selectinto;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InsertTabletPlansIterator {

  private static final Logger LOGGER = LoggerFactory.getLogger(InsertTabletPlansIterator.class);

  private static final Pattern LEVELED_PATH_NODE_PATTERN = Pattern.compile("\\$\\{\\w+}");

  private final QueryPlan queryPlan;
  private final QueryDataSet queryDataSet;

  private final PartialPath fromPath;
  private final List<PartialPath> intoPaths;
  private final boolean isIntoPathsAligned;

  private final int tabletRowLimit;

  private InsertTabletPlanGenerator[] insertTabletPlanGenerators;

  public InsertTabletPlansIterator(
      QueryPlan queryPlan,
      QueryDataSet queryDataSet,
      PartialPath fromPath,
      List<PartialPath> intoPaths,
      boolean isIntoPathsAligned)
      throws IllegalPathException {
    this.queryPlan = queryPlan;
    this.queryDataSet = queryDataSet;
    this.fromPath = fromPath;
    this.intoPaths = intoPaths;
    this.isIntoPathsAligned = isIntoPathsAligned;

    tabletRowLimit =
        IoTDBDescriptor.getInstance().getConfig().getSelectIntoInsertTabletPlanRowLimit();

    generateActualIntoPaths();
    constructInsertTabletPlanGenerators();
  }

  private void generateActualIntoPaths() throws IllegalPathException {
    for (int i = 0; i < intoPaths.size(); ++i) {
      intoPaths.set(i, generateActualIntoPath(i));
    }
  }

  private PartialPath generateActualIntoPath(int index) throws IllegalPathException {
    String[] nodes = fromPath.getNodes();
    StringBuffer sb = new StringBuffer();
    Matcher m = LEVELED_PATH_NODE_PATTERN.matcher(intoPaths.get(index).getFullPath());
    while (m.find()) {
      String param = m.group();
      String value = nodes[Integer.parseInt(param.substring(2, param.length() - 1).trim())];
      m.appendReplacement(sb, value == null ? "" : value);
    }
    m.appendTail(sb);
    return new PartialPath(sb.toString());
  }

  private void constructInsertTabletPlanGenerators() {
    final Map<String, Integer> sourcePathToQueryDataSetIndex = queryPlan.getPathToIndex();
    final List<String> sourcePaths = findSourcePaths();

    Map<String, InsertTabletPlanGenerator> deviceToPlanGeneratorMap = new HashMap<>();
    for (int i = 0, intoPathsSize = intoPaths.size(); i < intoPathsSize; i++) {
      String device = intoPaths.get(i).getDevice();
      if (!deviceToPlanGeneratorMap.containsKey(device)) {
        deviceToPlanGeneratorMap.put(
            device, new InsertTabletPlanGenerator(device, tabletRowLimit, isIntoPathsAligned));
      }
      deviceToPlanGeneratorMap
          .get(device)
          .collectTargetPathInformation(
              intoPaths.get(i).getMeasurement(),
              sourcePathToQueryDataSetIndex.get(sourcePaths.get(i)));
    }

    insertTabletPlanGenerators =
        deviceToPlanGeneratorMap.values().toArray(new InsertTabletPlanGenerator[0]);
  }

  private List<String> findSourcePaths() {
    // sourcePaths can be in queryPlanColumns or in queryDataSetPaths
    final List<ResultColumn> queryPlanColumns = queryPlan.getResultColumns();
    final List<Path> queryDataSetPaths = queryDataSet.getPaths();

    final Map<String, Integer> sourcePathToQueryDataSetIndex = queryPlan.getPathToIndex();
    final List<String> sourcePaths =
        new ArrayList<>(Math.max(queryPlanColumns.size(), queryDataSetPaths.size()));

    if (queryPlanColumns.size() == intoPaths.size()) {
      for (ResultColumn resultColumn : queryPlanColumns) {
        String path = resultColumn.getResultColumnName();
        if (!sourcePathToQueryDataSetIndex.containsKey(path)) {
          sourcePaths.clear();
          break;
        }
        sourcePaths.add(path);
      }
    }

    if (sourcePaths.isEmpty() && queryDataSetPaths.size() == intoPaths.size()) {
      for (Path path : queryDataSetPaths) {
        if (!sourcePathToQueryDataSetIndex.containsKey(path.getFullPath())) {
          sourcePaths.clear();
          break;
        }
        sourcePaths.add(path.getFullPath());
      }
    }

    if (sourcePaths.isEmpty()) {
      LOGGER.warn("select into: sourcePaths.isEmpty()");
    }

    return sourcePaths;
  }

  public boolean hasNext() throws IOException {
    return queryDataSet.hasNext();
  }

  public List<InsertTabletPlan> next() throws IOException, IllegalPathException {
    for (InsertTabletPlanGenerator insertTabletPlanGenerator : insertTabletPlanGenerators) {
      insertTabletPlanGenerator.internallyConstructNewPlan();
    }

    collectRowRecordIntoInsertTabletPlanGenerators();

    List<InsertTabletPlan> insertTabletPlans = new ArrayList<>();
    for (InsertTabletPlanGenerator insertTabletPlanGenerator : insertTabletPlanGenerators) {
      // all values can be null in a batch of the query dataset
      InsertTabletPlan insertTabletPlan = insertTabletPlanGenerator.generateInsertTabletPlan();
      if (insertTabletPlan.getColumns().length != 0) {
        insertTabletPlans.add(insertTabletPlan);
      }
    }
    return insertTabletPlans;
  }

  private void collectRowRecordIntoInsertTabletPlanGenerators() throws IOException {
    int count = 0;
    while (queryDataSet.hasNext() && count < tabletRowLimit) {
      RowRecord rowRecord = queryDataSet.next();
      for (InsertTabletPlanGenerator insertTabletPlanGenerator : insertTabletPlanGenerators) {
        insertTabletPlanGenerator.collectRowRecord(rowRecord);
      }
      ++count;
    }
  }
}
