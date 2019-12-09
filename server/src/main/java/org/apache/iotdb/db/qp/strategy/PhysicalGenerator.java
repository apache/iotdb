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
package org.apache.iotdb.db.qp.strategy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.executor.IQueryProcessExecutor;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.DeleteDataOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.InsertOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.db.qp.logical.sys.CreateTimeSeriesOperator;
import org.apache.iotdb.db.qp.logical.sys.DataAuthOperator;
import org.apache.iotdb.db.qp.logical.sys.DeleteStorageGroupOperator;
import org.apache.iotdb.db.qp.logical.sys.DeleteTimeSeriesOperator;
import org.apache.iotdb.db.qp.logical.sys.LoadDataOperator;
import org.apache.iotdb.db.qp.logical.sys.PropertyOperator;
import org.apache.iotdb.db.qp.logical.sys.SetStorageGroupOperator;
import org.apache.iotdb.db.qp.logical.sys.SetTTLOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowTTLOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DataAuthPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadDataPlan;
import org.apache.iotdb.db.qp.physical.sys.PropertyPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan.ShowContentType;
import org.apache.iotdb.db.qp.physical.sys.ShowTTLPlan;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;

/**
 * Used to convert logical operator to physical plan
 */
public class PhysicalGenerator {

  private IQueryProcessExecutor executor;

  public PhysicalGenerator(IQueryProcessExecutor executor) {
    this.executor = executor;
  }

  public PhysicalPlan transformToPhysicalPlan(Operator operator)
      throws QueryProcessException {
    List<Path> paths;
    switch (operator.getType()) {
      case AUTHOR:
        AuthorOperator author = (AuthorOperator) operator;
        try {
          return new AuthorPlan(author.getAuthorType(), author.getUserName(), author.getRoleName(),
              author.getPassWord(), author.getNewPassword(), author.getPrivilegeList(),
              author.getNodeName());
        } catch (AuthException e) {
          throw new QueryProcessException(e.getMessage());
        }
      case GRANT_WATERMARK_EMBEDDING:
      case REVOKE_WATERMARK_EMBEDDING:
        DataAuthOperator dataAuthOperator = (DataAuthOperator) operator;
        return new DataAuthPlan(dataAuthOperator.getType(), dataAuthOperator.getUsers());
      case LOADDATA:
        LoadDataOperator loadData = (LoadDataOperator) operator;
        return new LoadDataPlan(loadData.getInputFilePath(), loadData.getMeasureType());
      case METADATA:
      case SET_STORAGE_GROUP:
        SetStorageGroupOperator setStorageGroup = (SetStorageGroupOperator) operator;
        return new SetStorageGroupPlan(setStorageGroup.getPath());
      case DELETE_STORAGE_GROUP:
        DeleteStorageGroupOperator deleteStorageGroup = (DeleteStorageGroupOperator) operator;
        return new DeleteStorageGroupPlan(deleteStorageGroup.getDeletePathList());
      case CREATE_TIMESERIES:
        CreateTimeSeriesOperator addPath = (CreateTimeSeriesOperator) operator;
        return new CreateTimeSeriesPlan(addPath.getPath(), addPath.getDataType(),
            addPath.getEncoding(), addPath.getCompressor(), addPath.getProps());
      case DELETE_TIMESERIES:
        DeleteTimeSeriesOperator deletePath = (DeleteTimeSeriesOperator) operator;
        return new DeleteTimeSeriesPlan(deletePath.getDeletePathList());
      case PROPERTY:
        PropertyOperator property = (PropertyOperator) operator;
        return new PropertyPlan(property.getPropertyType(), property.getPropertyPath(),
            property.getMetadataPath());
      case DELETE:
        DeleteDataOperator delete = (DeleteDataOperator) operator;
        paths = delete.getSelectedPaths();
        return new DeletePlan(delete.getTime(), paths);
      case INSERT:
        InsertOperator insert = (InsertOperator) operator;
        paths = insert.getSelectedPaths();
        if (paths.size() != 1) {
          throw new LogicalOperatorException(
              "For Insert command, cannot specified more than one seriesPath: " + paths);
        }
        return new InsertPlan(paths.get(0).getFullPath(), insert.getTime(),
            insert.getMeasurementList(),
            insert.getValueList());
      case QUERY:
        QueryOperator query = (QueryOperator) operator;
        return transformQuery(query);
      case TTL:
        switch (operator.getTokenIntType()) {
          case SQLConstant.TOK_SET:
            SetTTLOperator setTTLOperator = (SetTTLOperator) operator;
            return new SetTTLPlan(setTTLOperator.getStorageGroup(), setTTLOperator.getDataTTL());
          case SQLConstant.TOK_UNSET:
            SetTTLOperator unsetTTLOperator = (SetTTLOperator) operator;
            return new SetTTLPlan(unsetTTLOperator.getStorageGroup());
          case SQLConstant.TOK_SHOW:
            ShowTTLOperator showTTLOperator = (ShowTTLOperator) operator;
            return new ShowTTLPlan(showTTLOperator.getStorageGroups());
          default:
            throw new LogicalOperatorException(String
                .format("not supported operator type %s in ttl operation.", operator.getType()));
        }
      case LOAD_CONFIGURATION:
        return new LoadConfigurationPlan();
      case SHOW:
        switch (operator.getTokenIntType()) {
          case SQLConstant.TOK_DYNAMIC_PARAMETER:
            return new ShowPlan(ShowContentType.DYNAMIC_PARAMETER);
          case SQLConstant.TOK_FLUSH_TASK_INFO:
            return new ShowPlan(ShowContentType.FLUSH_TASK_INFO);
          default:
            throw new LogicalOperatorException(String
                .format("not supported operator type %s in show operation.", operator.getType()));
        }
      default:
        throw new LogicalOperatorException(operator.getType().toString(), "");
    }
  }

  protected TSDataType getSeriesType(String path) throws QueryProcessException, MetadataException {
    return SchemaUtils.getSeriesType(path);
  }

  private PhysicalPlan transformQuery(QueryOperator queryOperator)
      throws QueryProcessException {
    QueryPlan queryPlan;

    if (queryOperator.isGroupBy()) {
      queryPlan = new GroupByPlan();
      ((GroupByPlan) queryPlan).setUnit(queryOperator.getUnit());
      ((GroupByPlan) queryPlan).setOrigin(queryOperator.getOrigin());
      ((GroupByPlan) queryPlan).setIntervals(queryOperator.getIntervals());
      ((GroupByPlan) queryPlan)
          .setAggregations(queryOperator.getSelectOperator().getAggregations());
    } else if (queryOperator.isFill()) {
      queryPlan = new FillQueryPlan();
      FilterOperator timeFilter = queryOperator.getFilterOperator();
      if (!timeFilter.isSingle()) {
        throw new QueryProcessException("Slice query must select a single time point");
      }
      long time = Long.parseLong(((BasicFunctionOperator) timeFilter).getValue());
      ((FillQueryPlan) queryPlan).setQueryTime(time);
      ((FillQueryPlan) queryPlan).setFillType(queryOperator.getFillTypes());
    } else if (queryOperator.hasAggregation()) { // ordinary query
      queryPlan = new AggregationPlan();
      ((AggregationPlan) queryPlan)
          .setAggregations(queryOperator.getSelectOperator().getAggregations());
    } else {
      queryPlan = new QueryPlan();
    }

    if (queryOperator.isGroupByDevice()) {
      // below is the core realization of GROUP_BY_DEVICE sql logic
      List<Path> prefixPaths = queryOperator.getFromOperator().getPrefixPaths();
      List<Path> suffixPaths = queryOperator.getSelectOperator().getSuffixPaths();
      List<String> originAggregations = queryOperator.getSelectOperator().getAggregations();

      List<String> measurementColumnList = new ArrayList<>();
      Map<String, Set<String>> measurementColumnsGroupByDevice = new LinkedHashMap<>();
      Map<String, TSDataType> dataTypeConsistencyChecker = new HashMap<>();
      Set<Path> allSelectPaths = new HashSet<>();

      for (int i = 0; i < suffixPaths.size(); i++) { // per suffix
        Path suffixPath = suffixPaths.get(i);
        Set<String> deviceSetOfGivenSuffix = new HashSet<>();
        Set<String> measurementSetOfGivenSuffix = new TreeSet<>();
        for (Path prefixPath : prefixPaths) { // per prefix
          Path fullPath = Path.addPrefixPath(suffixPath, prefixPath);
          Set<String> tmpDeviceSet = new HashSet<>();
          try {
            List<String> actualPaths = executor.getAllPaths(fullPath.getFullPath());
            for (String pathStr : actualPaths) {
              Path path = new Path(pathStr);
              String device = path.getDevice();
              // update tmpDeviceSet for a full path
              tmpDeviceSet.add(device);

              // ignore the duplicate prefix device for a given suffix path
              // e.g. select s0 from root.vehicle.d0, root.vehicle.d0 group by device,
              // for the given suffix path "s0", the second prefix device "root.vehicle.d0" is
              // duplicated and should be neglected.
              if (deviceSetOfGivenSuffix.contains(device)) {
                continue;
              }

              // get pathForDataType and measurementColumn
              String measurement = path.getMeasurement();
              String pathForDataType;
              String measurementColumn;
              if (originAggregations != null && !originAggregations.isEmpty()) {
                pathForDataType = originAggregations.get(i) + "(" + path.getFullPath() + ")";
                measurementColumn = originAggregations.get(i) + "(" + measurement + ")";
              } else {
                pathForDataType = path.getFullPath();
                measurementColumn = measurement;
              }
              // check the consistency of data types
              // a example of inconsistency: select s0 from root.sg1.d1, root.sg2.d3 group by device,
              // while root.sg1.d1.s0 is INT32 and root.sg2.d3.s0 is FLOAT.
              TSDataType dataType = getSeriesType(pathForDataType);
              if (dataTypeConsistencyChecker.containsKey(measurementColumn)) {
                if (!dataType.equals(dataTypeConsistencyChecker.get(measurementColumn))) {
                  throw new QueryProcessException(
                      "The data types of the same measurement column should be the same across "
                          + "devices in GROUP_BY_DEVICE sql. For more details please refer to the "
                          + "SQL document.");
                }
              } else {
                dataTypeConsistencyChecker.put(measurementColumn, dataType);
              }
              // update measurementSetOfGivenSuffix
              measurementSetOfGivenSuffix.add(measurementColumn);

              // update measurementColumnsGroupByDevice
              if (!measurementColumnsGroupByDevice.containsKey(device)) {
                measurementColumnsGroupByDevice.put(device, new HashSet<>());
              }
              measurementColumnsGroupByDevice.get(device).add(measurementColumn);

              // update allSelectedPaths
              allSelectPaths.add(path);
            }
            // update deviceSetOfGivenSuffix
            deviceSetOfGivenSuffix.addAll(tmpDeviceSet);

          } catch (MetadataException e) {
            throw new LogicalOptimizeException(
                String.format("Error when getting all paths of a full path: %s",
                    fullPath.getFullPath()) + e.getMessage());
          }
        }
        // update measurementColumnList
        // Note that in the loop of a suffix path, set is used.
        // And across the loops of suffix paths, list is used.
        // e.g. select *,s1 from root.sg.d0, root.sg.d1
        // for suffix *, measurementSetOfGivenSuffix = {s1,s2,s3}
        // for suffix s1, measurementSetOfGivenSuffix = {s1}
        // therefore the final measurementColumnList is [s1,s2,s3,s1].
        measurementColumnList.addAll(measurementSetOfGivenSuffix);
      }

      if (measurementColumnList.isEmpty()) {
        throw new QueryProcessException("do not select any existing series");
      }

      // slimit trim on the measurementColumnList
      if (queryOperator.hasSlimit()) {
        int seriesSlimit = queryOperator.getSeriesLimit();
        int seriesOffset = queryOperator.getSeriesOffset();
        measurementColumnList = slimitTrimColumn(measurementColumnList, seriesSlimit, seriesOffset);
      }

      // assigns to queryPlan
      queryPlan.setGroupByDevice(true);
      queryPlan.setMeasurementColumnList(measurementColumnList);
      queryPlan.setMeasurementColumnsGroupByDevice(measurementColumnsGroupByDevice);
      queryPlan.setDataTypeConsistencyChecker(dataTypeConsistencyChecker);
      queryPlan.setPaths(new ArrayList<>(allSelectPaths));
    } else {
      List<Path> paths = queryOperator.getSelectedPaths();
      queryPlan.setPaths(paths);
    }

    queryPlan.checkPaths(executor);

    // transform filter operator to expression
    FilterOperator filterOperator = queryOperator.getFilterOperator();

    if (filterOperator != null) {
      IExpression expression = filterOperator.transformToExpression(executor);
      queryPlan.setExpression(expression);
    }

    queryPlan.setRowLimit(queryOperator.getRowLimit());
    queryPlan.setRowOffset(queryOperator.getRowOffset());

    return queryPlan;
  }


  private List<String> slimitTrimColumn(List<String> columnList, int seriesLimit, int seriesOffset)
      throws QueryProcessException {
    int size = columnList.size();

    // check parameter range
    if (seriesOffset >= size) {
      throw new QueryProcessException("SOFFSET <SOFFSETValue>: SOFFSETValue exceeds the range.");
    }
    int endPosition = seriesOffset + seriesLimit;
    if (endPosition > size) {
      endPosition = size;
    }

    // trim seriesPath list
    return new ArrayList<>(columnList.subList(seriesOffset, endPosition));
  }

}

