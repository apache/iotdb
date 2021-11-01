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
package org.apache.iotdb.db.qp.logical.crud;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.VectorPartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan.MeasurementType;
import org.apache.iotdb.db.qp.physical.crud.MeasurementInfo;
import org.apache.iotdb.db.qp.physical.crud.QueryIndexPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.utils.SchemaUtils.getAggregationType;

public class QueryOperator extends Operator {

  protected SelectComponent selectComponent;
  protected FromComponent fromComponent;
  protected WhereComponent whereComponent;
  protected SpecialClauseComponent specialClauseComponent;

  protected Map<String, Object> props;
  protected IndexType indexType;

  protected boolean enableTracing;

  public QueryOperator() {
    super(SQLConstant.TOK_QUERY);
    operatorType = Operator.OperatorType.QUERY;
  }

  public QueryOperator(QueryOperator queryOperator) {
    this();
    this.selectComponent = queryOperator.getSelectComponent();
    this.fromComponent = queryOperator.getFromComponent();
    this.whereComponent = queryOperator.getWhereComponent();
    this.specialClauseComponent = queryOperator.getSpecialClauseComponent();
    this.props = queryOperator.getProps();
    this.indexType = queryOperator.getIndexType();
    this.enableTracing = queryOperator.isEnableTracing();
  }

  public SelectComponent getSelectComponent() {
    return selectComponent;
  }

  public void setSelectComponent(SelectComponent selectComponent) {
    this.selectComponent = selectComponent;
  }

  public FromComponent getFromComponent() {
    return fromComponent;
  }

  public void setFromComponent(FromComponent fromComponent) {
    this.fromComponent = fromComponent;
  }

  public WhereComponent getWhereComponent() {
    return whereComponent;
  }

  public void setWhereComponent(WhereComponent whereComponent) {
    this.whereComponent = whereComponent;
  }

  public void setSpecialClauseComponent(SpecialClauseComponent specialClauseComponent) {
    this.specialClauseComponent = specialClauseComponent;
  }

  public SpecialClauseComponent getSpecialClauseComponent() {
    return specialClauseComponent;
  }

  public Map<String, Object> getProps() {
    return props;
  }

  public void setProps(Map<String, Object> props) {
    this.props = props;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public void setIndexType(IndexType indexType) {
    this.indexType = indexType;
  }

  public boolean hasAggregationFunction() {
    return selectComponent.hasAggregationFunction();
  }

  public boolean hasTimeSeriesGeneratingFunction() {
    return selectComponent.hasTimeSeriesGeneratingFunction();
  }

  public boolean isAlignByDevice() {
    return specialClauseComponent != null && specialClauseComponent.isAlignByDevice();
  }

  public boolean isAlignByTime() {
    return specialClauseComponent == null || specialClauseComponent.isAlignByTime();
  }

  public boolean isGroupByLevel() {
    return specialClauseComponent != null && specialClauseComponent.getLevels() != null;
  }

  public int[] getLevels() {
    return specialClauseComponent.getLevels();
  }

  public boolean hasSlimit() {
    return specialClauseComponent != null && specialClauseComponent.hasSlimit();
  }

  public boolean hasSoffset() {
    return specialClauseComponent != null && specialClauseComponent.hasSoffset();
  }

  /** Reset sLimit and sOffset to zero. */
  public void resetSLimitOffset() {
    if (specialClauseComponent != null) {
      specialClauseComponent.setSeriesLimit(0);
      specialClauseComponent.setSeriesOffset(0);
    }
  }

  public void check() throws LogicalOperatorException {
    if (isAlignByDevice() && selectComponent.hasTimeSeriesGeneratingFunction()) {
      throw new LogicalOperatorException("ALIGN BY DEVICE clause is not supported in UDF queries.");
    }
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    QueryPlan queryPlan = indexType == null ? new RawDataQueryPlan() : new QueryIndexPlan();
    return isAlignByDevice()
        ? this.generateAlignByDevicePlan(generator)
        : this.generateRawDataQueryPlan(generator, queryPlan);
  }

  protected QueryPlan generateRawDataQueryPlan(PhysicalGenerator generator, QueryPlan queryPlan)
      throws QueryProcessException {
    RawDataQueryPlan rawDataQueryPlan = (RawDataQueryPlan) queryPlan;
    rawDataQueryPlan.setPaths(selectComponent.getPaths());
    rawDataQueryPlan.setResultColumns(selectComponent.getResultColumns());
    rawDataQueryPlan.setEnableTracing(enableTracing);

    // transform filter operator to expression
    if (whereComponent != null) {
      FilterOperator filterOperator = whereComponent.getFilterOperator();
      List<PartialPath> filterPaths = new ArrayList<>(filterOperator.getPathSet());
      try {
        List<TSDataType> seriesTypes = generator.getSeriesTypes(filterPaths);
        HashMap<PartialPath, TSDataType> pathTSDataTypeHashMap = new HashMap<>();
        for (int i = 0; i < filterPaths.size(); i++) {
          rawDataQueryPlan.addFilterPathInDeviceToMeasurements(filterPaths.get(i));
          pathTSDataTypeHashMap.put(filterPaths.get(i), seriesTypes.get(i));
        }
        IExpression expression = filterOperator.transformToExpression(pathTSDataTypeHashMap);
        rawDataQueryPlan.setExpression(expression);
      } catch (MetadataException e) {
        throw new LogicalOptimizeException(e.getMessage());
      }
    }

    if (queryPlan instanceof QueryIndexPlan) {
      ((QueryIndexPlan) queryPlan).setIndexType(indexType);
      ((QueryIndexPlan) queryPlan).setProps(props);
      return queryPlan;
    }

    try {
      rawDataQueryPlan.setDataTypes(generator.getSeriesTypes(selectComponent.getPaths()));
      queryPlan.deduplicate(generator);
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }

    convertSpecialClauseValues(rawDataQueryPlan);

    return rawDataQueryPlan;
  }

  protected AlignByDevicePlan generateAlignByDevicePlan(PhysicalGenerator generator)
      throws QueryProcessException {
    AlignByDevicePlan alignByDevicePlan = new AlignByDevicePlan();

    List<PartialPath> prefixPaths = fromComponent.getPrefixPaths();
    // remove stars in fromPaths and get deviceId with deduplication
    List<PartialPath> devices = removeStarsInDeviceWithUnique(prefixPaths);
    List<ResultColumn> resultColumns = selectComponent.getResultColumns();
    List<String> aggregationFuncs = selectComponent.getAggregationFunctions();
    // to record result measurement columns
    List<String> measurements = new ArrayList<>();
    Map<String, MeasurementInfo> measurementInfoMap = new HashMap<>();
    List<PartialPath> paths = new ArrayList<>();

    for (int i = 0; i < resultColumns.size(); i++) { // per suffix in SELECT
      ResultColumn resultColumn = resultColumns.get(i);
      Expression suffixExpression = resultColumn.getExpression();
      PartialPath suffixPath = getSuffixPathFromExpression(suffixExpression);
      String aggregation = aggregationFuncs != null ? aggregationFuncs.get(i) : null;

      // if const measurement
      if (suffixPath.getMeasurement().startsWith("'")) {
        String measurementName = suffixPath.getMeasurement();
        measurements.add(measurementName);
        measurementInfoMap.put(measurementName, new MeasurementInfo(MeasurementType.Constant));
        continue;
      }

      // to record measurements in the loop of a suffix path
      Set<String> measurementSetOfGivenSuffix = new LinkedHashSet<>();
      for (PartialPath device : devices) {
        PartialPath fullPath = device.concatPath(suffixPath);
        try {
          // remove stars in SELECT to get actual paths
          List<PartialPath> actualPaths = getMatchedTimeseries(fullPath);
          if (suffixPath.getNodes().length > 1
              && (actualPaths.isEmpty() || !(actualPaths.get(0) instanceof VectorPartialPath))) {
            throw new QueryProcessException(AlignByDevicePlan.MEASUREMENT_ERROR_MESSAGE);
          }
          if (resultColumn.hasAlias() && actualPaths.size() >= 2) {
            throw new QueryProcessException(
                String.format(AlignByDevicePlan.ALIAS_ERROR_MESSAGE, resultColumn.getAlias()));
          }
          if (actualPaths.isEmpty()) {
            String nonExistMeasurement = getMeasurementName(fullPath, aggregation);
            if (measurementSetOfGivenSuffix.add(nonExistMeasurement)) {
              measurementInfoMap.putIfAbsent(
                  nonExistMeasurement, new MeasurementInfo(MeasurementType.NonExist));
            }
          } else {
            for (PartialPath path : actualPaths) {
              String measurementName = getMeasurementName(path, aggregation);
              TSDataType measurementDataType = IoTDB.metaManager.getSeriesType(path);
              TSDataType columnDataType = getAggregationType(aggregation);
              columnDataType = columnDataType == null ? measurementDataType : columnDataType;
              MeasurementInfo measurementInfo =
                  measurementInfoMap.getOrDefault(measurementName, new MeasurementInfo());

              if (resultColumn.hasAlias()) {
                measurementInfo.setMeasurementAlias(resultColumn.getAlias());
              }

              // check datatype consistency
              // an inconsistent example: select s0 from root.sg1.d1, root.sg1.d2 align by device
              // while root.sg1.d1.s0 is INT32 and root.sg1.d2.s0 is FLOAT.
              if (measurementInfo.getColumnDataType() != null) {
                if (!columnDataType.equals(measurementInfo.getColumnDataType())) {
                  throw new QueryProcessException(
                      "The data types of the same measurement column should be the same across devices.");
                }
              } else {
                measurementInfo.setColumnDataType(columnDataType);
                measurementInfo.setMeasurementDataType(measurementDataType);
              }

              measurementSetOfGivenSuffix.add(measurementName);
              measurementInfo.setMeasurementType(MeasurementType.Exist);
              measurementInfoMap.put(measurementName, measurementInfo);
              // update paths
              paths.add(path);
            }
          }
        } catch (MetadataException | QueryProcessException e) {
          throw new QueryProcessException(e.getMessage());
        }
      }

      // Note that in the loop of a suffix path, set is used.
      // And across the loops of suffix paths, list is used.
      // e.g. select *,s1 from root.sg.d0, root.sg.d1
      // for suffix *, measurementSetOfGivenSuffix = {s1,s2,s3}
      // for suffix s1, measurementSetOfGivenSuffix = {s1}
      // therefore the final measurements is [s1,s2,s3,s1].
      measurements.addAll(measurementSetOfGivenSuffix);
    }

    List<String> trimMeasurements = convertSpecialClauseValues(alignByDevicePlan, measurements);
    // assigns to alignByDevicePlan
    alignByDevicePlan.setMeasurements(trimMeasurements);
    alignByDevicePlan.setMeasurementInfoMap(measurementInfoMap);
    alignByDevicePlan.setDevices(devices);
    alignByDevicePlan.setPaths(paths);
    alignByDevicePlan.setEnableTracing(enableTracing);

    if (whereComponent != null) {
      alignByDevicePlan.setDeviceToFilterMap(
          concatFilterByDevice(generator, devices, whereComponent.getFilterOperator()));
    }

    return alignByDevicePlan;
  }

  private void convertSpecialClauseValues(QueryPlan queryPlan) {
    if (specialClauseComponent != null) {
      queryPlan.setWithoutAllNull(specialClauseComponent.isWithoutAllNull());
      queryPlan.setWithoutAnyNull(specialClauseComponent.isWithoutAnyNull());
      queryPlan.setRowLimit(specialClauseComponent.getRowLimit());
      queryPlan.setRowOffset(specialClauseComponent.getRowOffset());
      queryPlan.setAscending(specialClauseComponent.isAscending());
      queryPlan.setAlignByTime(specialClauseComponent.isAlignByTime());
    }
  }

  private List<String> convertSpecialClauseValues(QueryPlan queryPlan, List<String> measurements)
      throws QueryProcessException {
    convertSpecialClauseValues(queryPlan);
    // sLimit trim on the measurementColumnList
    if (specialClauseComponent.hasSlimit()) {
      int seriesSLimit = specialClauseComponent.getSeriesLimit();
      int seriesOffset = specialClauseComponent.getSeriesOffset();
      return slimitTrimColumn(measurements, seriesSLimit, seriesOffset);
    }
    return measurements;
  }

  private List<PartialPath> removeStarsInDeviceWithUnique(List<PartialPath> paths)
      throws LogicalOptimizeException {
    List<PartialPath> retDevices;
    Set<PartialPath> deviceSet = new LinkedHashSet<>();
    try {
      for (PartialPath path : paths) {
        Set<PartialPath> tempDS = getMatchedDevices(path);
        deviceSet.addAll(tempDS);
      }
      retDevices = new ArrayList<>(deviceSet);
    } catch (MetadataException e) {
      throw new LogicalOptimizeException("error when remove star: " + e.getMessage());
    }
    return retDevices;
  }

  private PartialPath getSuffixPathFromExpression(Expression expression) {
    return expression instanceof TimeSeriesOperand
        ? ((TimeSeriesOperand) expression).getPath()
        : (((FunctionExpression) expression).getPaths().get(0));
  }

  /**
   * If path is a vectorPartialPath, we return its measurementId + subMeasurement as the final
   * measurement. e.g. path: root.sg.d1.vector1[s1], return "vector1.s1".
   */
  private String getMeasurementName(PartialPath path, String aggregation) {
    String initialMeasurement = path.getMeasurement();
    if (path instanceof VectorPartialPath) {
      String subMeasurement = ((VectorPartialPath) path).getSubSensor(0);
      initialMeasurement += TsFileConstant.PATH_SEPARATOR + subMeasurement;
    }
    if (aggregation != null) {
      initialMeasurement = aggregation + "(" + initialMeasurement + ")";
    }
    return initialMeasurement;
  }

  private List<String> slimitTrimColumn(List<String> columnList, int seriesLimit, int seriesOffset)
      throws QueryProcessException {
    int size = columnList.size();

    // check parameter range
    if (seriesOffset >= size) {
      String errorMessage =
          "The value of SOFFSET (%d) is equal to or exceeds the number of sequences (%d) that can actually be returned.";
      throw new QueryProcessException(String.format(errorMessage, seriesOffset, size));
    }
    int endPosition = seriesOffset + seriesLimit;
    if (endPosition > size) {
      endPosition = size;
    }

    // trim seriesPath list
    return new ArrayList<>(columnList.subList(seriesOffset, endPosition));
  }

  // e.g. translate "select * from root.ln.d1, root.ln.d2 where s1 < 20 AND s2 > 10" to
  // [root.ln.d1 -> root.ln.d1.s1 < 20 AND root.ln.d1.s2 > 10,
  //  root.ln.d2 -> root.ln.d2.s1 < 20 AND root.ln.d2.s2 > 10)]
  private Map<String, IExpression> concatFilterByDevice(
      PhysicalGenerator generator, List<PartialPath> devices, FilterOperator operator)
      throws QueryProcessException {
    Map<String, IExpression> deviceToFilterMap = new HashMap<>();
    Set<PartialPath> filterPaths = new HashSet<>();
    Iterator<PartialPath> deviceIterator = devices.iterator();
    while (deviceIterator.hasNext()) {
      PartialPath device = deviceIterator.next();
      FilterOperator newOperator = operator.copy();
      try {
        concatFilterPath(device, newOperator, filterPaths);
      } catch (PathNotExistException e) {
        deviceIterator.remove();
        continue;
      }
      // transform to a list so it can be indexed
      List<PartialPath> filterPathList = new ArrayList<>(filterPaths);
      try {
        List<TSDataType> seriesTypes = generator.getSeriesTypes(filterPathList);
        Map<PartialPath, TSDataType> pathTSDataTypeHashMap = new HashMap<>();
        for (int i = 0; i < filterPathList.size(); i++) {
          pathTSDataTypeHashMap.put(filterPathList.get(i), seriesTypes.get(i));
        }
        deviceToFilterMap.put(
            device.getFullPath(), newOperator.transformToExpression(pathTSDataTypeHashMap));
        filterPaths.clear();
      } catch (MetadataException e) {
        throw new QueryProcessException(e);
      }
    }

    return deviceToFilterMap;
  }

  private void concatFilterPath(
      PartialPath prefix, FilterOperator operator, Set<PartialPath> filterPaths)
      throws PathNotExistException {
    if (!operator.isLeaf()) {
      for (FilterOperator child : operator.getChildren()) {
        concatFilterPath(prefix, child, filterPaths);
      }
      return;
    }
    FunctionOperator basicOperator;
    if (operator instanceof InOperator) {
      basicOperator = (InOperator) operator;
    } else if (operator instanceof LikeOperator) {
      basicOperator = (LikeOperator) operator;
    } else if (operator instanceof RegexpOperator) {
      basicOperator = (RegexpOperator) operator;
    } else {
      basicOperator = (BasicFunctionOperator) operator;
    }
    PartialPath filterPath = basicOperator.getSinglePath();

    // do nothing in the cases of "where time > 5" or "where root.d1.s1 > 5"
    if (SQLConstant.isReservedPath(filterPath)
        || filterPath.getFirstNode().startsWith(SQLConstant.ROOT)) {
      filterPaths.add(filterPath);
      return;
    }

    PartialPath concatPath = prefix.concatPath(filterPath);
    if (IoTDB.metaManager.isPathExist(concatPath)) {
      filterPaths.add(concatPath);
      basicOperator.setSinglePath(concatPath);
    } else {
      throw new PathNotExistException(concatPath.getFullPath());
    }
  }

  protected Set<PartialPath> getMatchedDevices(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getMatchedDevices(path);
  }

  protected List<PartialPath> getMatchedTimeseries(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getFlatMeasurementPaths(path);
  }

  public boolean isEnableTracing() {
    return enableTracing;
  }

  public void setEnableTracing(boolean enableTracing) {
    this.enableTracing = enableTracing;
  }
}
