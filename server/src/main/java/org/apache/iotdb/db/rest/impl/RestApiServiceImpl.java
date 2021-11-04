/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.rest.impl;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.db.qp.physical.sys.SetSystemModePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.rest.RestApiService;
import org.apache.iotdb.db.rest.model.DataJsonInner;
import org.apache.iotdb.db.rest.model.InsertRow;
import org.apache.iotdb.db.rest.model.QueryData;
import org.apache.iotdb.db.rest.model.ResponseResult;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class RestApiServiceImpl extends RestApiService {
  private static final Logger LOGGER = LoggerFactory.getLogger(RestApiServiceImpl.class);
  protected IPlanExecutor executor;

  public RestApiServiceImpl() throws QueryProcessException {
    executor = new PlanExecutor();
  }

  @Override
  public Response executeNonQueryStatement(QueryData queryData, SecurityContext securityContext) {
    boolean executorStatus;
    ResponseResult responseResult;
    try {
      Planner planner = new Planner();
      PhysicalPlan physicalPlan = planner.parseSQLToPhysicalPlan(queryData.getSql());
      executorStatus = executor.processNonQuery(physicalPlan);
    } catch (Exception e) {
      return Response.ok().entity(tryCatchNonQueryException(e)).build();
    }
    if (executorStatus) {
      responseResult =
          getResponseResult(
              TSStatusCode.SUCCESS_STATUS.getStatusCode(), TSStatusCode.SUCCESS_STATUS.name());
    } else {
      responseResult =
          getResponseResult(
              TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(),
              TSStatusCode.EXECUTE_STATEMENT_ERROR.name());
    }
    return Response.ok().entity(responseResult).build();
  }

  private QueryDataSet getDataBySelect(PhysicalPlan physicalPlan)
      throws TException, StorageEngineException, QueryFilterOptimizationException,
          MetadataException, IOException, InterruptedException, SQLException,
          QueryProcessException {
    long quertId = QueryResourceManager.getInstance().assignQueryId(true);
    QueryContext context = new QueryContext(quertId);
    return executor.processQuery(physicalPlan, context);
  }

  @Override
  public Response executeQueryStatement(QueryData queryData, SecurityContext securityContext) {
    List<DataJsonInner> dataFrameInnerList = new ArrayList<>();
    try {
      Planner planner = new Planner();
      PhysicalPlan physicalPlan = planner.parseSQLToPhysicalPlan(queryData.getSql());
      if (!(physicalPlan instanceof QueryPlan)) {
        return Response.ok()
            .entity(
                getResponseResult(
                    TSStatusCode.QUERY_NOT_ALLOWED.getStatusCode(),
                    TSStatusCode.QUERY_NOT_ALLOWED.name()))
            .build();
      }
      if (!AuthorityChecker.check(
          securityContext.getUserPrincipal().getName(),
          physicalPlan.getPaths(),
          physicalPlan.getOperatorType(),
          null)) {
        return Response.ok()
            .entity(
                getResponseResult(
                    TSStatusCode.NO_PERMISSION_ERROR.getStatusCode(),
                    TSStatusCode.NO_PERMISSION_ERROR.name()))
            .build();
      }
      QueryDataSet dataSet = getDataBySelect(physicalPlan);
      Map<String, List<Object>> pathsMap = new LinkedHashMap<>();
      while (dataSet != null && dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        List<Field> fields = rowRecord.getFields();
        int i = 0;
        for (Field field : fields) {
          List<Object> dataValues = new ArrayList<>();
          if (field == null || field.getDataType() == null) {
            dataValues.add(null);
          } else {
            if (field.getDataType().name().equals("TEXT")) {
              dataValues.add(field.getStringValue());
            } else {
              dataValues.add(field.getObjectValue(field.getDataType()));
            }
          }
          dataValues.add(rowRecord.getTimestamp());
          if (pathsMap.get(dataSet.getPaths().get(i).getFullPath()) == null) {
            List<Object> dataSubValues = new ArrayList<>();
            dataSubValues.add(dataValues);
            pathsMap.put(dataSet.getPaths().get(i).getFullPath(), dataSubValues);
          } else {
            pathsMap.get(dataSet.getPaths().get(i).getFullPath()).add(dataValues);
          }
          i++;
        }
      }
      Set<Entry<String, List<Object>>> entrySet = pathsMap.entrySet();
      for (Map.Entry<String, List<Object>> entry : entrySet) {
        DataJsonInner dataFrameInner = new DataJsonInner();
        dataFrameInner.setDataValues(entry.getValue());
        dataFrameInner.setMeasurements(entry.getKey());
        dataFrameInnerList.add(dataFrameInner);
      }
    } catch (Exception e) {
      return Response.ok().entity(tryCatchNonQueryException(e)).build();
    }
    return Response.ok().entity(dataFrameInnerList).build();
  }

  @Override
  public Response insertTablet(InsertRow insertRow, SecurityContext securityContext) {
    InsertTabletPlan insertTabletPlan;
    boolean authorizationStatus;
    boolean executeStatus;
    ResponseResult responseResult;
    try {
      insertTabletPlan =
          new InsertTabletPlan(
              new PartialPath(insertRow.getDeviceId()), insertRow.getMeasurements());
      insertTabletPlan.setTimes(
          insertRow.getTimestamps().stream().mapToLong(Long::longValue).toArray());
      List<List<String>> listData = insertRow.getValues();
      List<String> listDataType = insertRow.getDataType();
      Object[] columns = new Object[listDataType.size()];
      List<Integer> tsDataType = new ArrayList<>();
      int size = insertRow.getRowSize();
      BitMap[] bitMaps = new BitMap[listDataType.size()];
      for (int i = 0; i < listDataType.size(); i++) {
        bitMaps[i] = new BitMap(size);
        switch (listDataType.get(i)) {
          case "BOOLEAN":
            boolean[] booleanValues = new boolean[size];
            for (int index = 0; index < size; index++) {
              if (listData.get(i).get(index) == null) {
                bitMaps[i].mark(index);
              } else {
                booleanValues[index] = Boolean.parseBoolean(listData.get(i).get(index));
              }
            }
            columns[i] = booleanValues;
            tsDataType.add(TSDataType.BOOLEAN.ordinal());
            break;
          case "INT32":
            int[] intValues = new int[size];
            for (int index = 0; index < size; index++) {
              if (listData.get(i).get(index) == null) {
                bitMaps[i].mark(index);
              } else {
                intValues[index] = Integer.parseInt(listData.get(i).get(index));
              }
            }
            columns[i] = intValues;
            tsDataType.add(TSDataType.INT32.ordinal());
            break;
          case "INT64":
            long[] longValues = new long[size];
            for (int index = 0; index < size; index++) {
              if (listData.get(i).get(index) == null) {
                bitMaps[i].mark(index);
              } else {
                longValues[index] = Long.parseLong(listData.get(i).get(index));
              }
            }
            columns[i] = longValues;
            tsDataType.add(TSDataType.INT64.ordinal());
            break;
          case "FLOAT":
            float[] floatValues = new float[size];
            for (int index = 0; index < size; index++) {
              if (listData.get(i).get(index) == null) {
                bitMaps[i].mark(index);
              } else {
                floatValues[index] = Float.parseFloat(listData.get(i).get(index));
              }
            }
            columns[i] = floatValues;
            tsDataType.add(TSDataType.FLOAT.ordinal());
            break;
          case "DOUBLE":
            double[] doubleValues = new double[size];
            for (int index = 0; index < size; index++) {
              if (listData.get(i).get(index) == null) {
                bitMaps[i].mark(index);
              } else {
                doubleValues[index] = Double.parseDouble(listData.get(i).get(index));
              }
            }
            columns[i] = doubleValues;
            tsDataType.add(TSDataType.DOUBLE.ordinal());
            break;
          case "TEXT":
            Binary[] binaryValues = new Binary[listData.get(i).size()];
            for (int index = 0; index < listData.get(i).size(); index++) {
              if (listData.get(i).get(index) == null) {
                bitMaps[i].mark(index);
                binaryValues[index] = new Binary("".getBytes(StandardCharsets.UTF_8));
              } else {
                binaryValues[index] =
                    new Binary(listData.get(i).get(index).getBytes(StandardCharsets.UTF_8));
              }
            }
            columns[i] = binaryValues;
            tsDataType.add(TSDataType.TEXT.ordinal());
            break;
          default:
            throw new IllegalArgumentException("Invalid input: " + listDataType.get(i));
        }
      }
      insertTabletPlan.setColumns(columns);
      insertTabletPlan.setBitMaps(bitMaps);
      insertTabletPlan.setRowCount(insertRow.getRowSize());
      insertTabletPlan.setDataTypes(tsDataType);
      insertTabletPlan.setAligned(insertRow.getIsAligned());
      PartialPath partialPath = new PartialPath(insertRow.getDeviceId());
      List<PartialPath> paths = new ArrayList<>();
      paths.add(partialPath);
      authorizationStatus =
          checkAuthorization(paths, insertTabletPlan, securityContext.getUserPrincipal().getName());
      if (authorizationStatus) {
        executeStatus = executeNonQuery(insertTabletPlan);
        if (executeStatus) {
          responseResult =
              getResponseResult(
                  TSStatusCode.SUCCESS_STATUS.getStatusCode(), TSStatusCode.SUCCESS_STATUS.name());
        } else {
          responseResult =
              getResponseResult(
                  TSStatusCode.WRITE_PROCESS_ERROR.getStatusCode(),
                  TSStatusCode.WRITE_PROCESS_ERROR.name());
        }
      } else {
        responseResult =
            getResponseResult(
                TSStatusCode.NO_PERMISSION_ERROR.getStatusCode(),
                TSStatusCode.NO_PERMISSION_ERROR.name());
        return Response.ok().entity(responseResult).build();
      }
    } catch (Exception e) {
      responseResult = tryCatchNonQueryException(e);
    }
    return Response.ok().entity(responseResult).build();
  }

  private ResponseResult getResponseResult(int code, String message) {
    ResponseResult responseResult = new ResponseResult();
    responseResult.setCode(code);
    responseResult.setMessage(message);
    return responseResult;
  }

  private boolean checkAuthorization(List<PartialPath> paths, PhysicalPlan plan, String username)
      throws AuthException {
    String targetUser = null;
    if (plan instanceof AuthorPlan) {
      targetUser = ((AuthorPlan) plan).getUserName();
    }
    return AuthorityChecker.check(username, paths, plan.getOperatorType(), targetUser);
  }

  private boolean executeNonQuery(PhysicalPlan plan)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    if (!(plan instanceof SetSystemModePlan)
        && !(plan instanceof FlushPlan)
        && IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new QueryProcessException(
          "Current system mode is read-only, does not support non-query operation");
    }
    return executor.processNonQuery(plan);
  }

  private ResponseResult tryCatchNonQueryException(Exception e) {
    ResponseResult responseResult = new ResponseResult();
    if (e instanceof QueryProcessException) {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(((QueryProcessException) e).getErrorCode());
    } else if (e instanceof StorageGroupNotSetException) {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(((StorageGroupNotSetException) e).getErrorCode());
    } else if (e instanceof StorageEngineException) {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(((StorageEngineException) e).getErrorCode());
    } else if (e instanceof AuthException) {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(Status.BAD_REQUEST.getStatusCode());
    } else if (e instanceof IllegalPathException) {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(((IllegalPathException) e).getErrorCode());
    } else if (e instanceof MetadataException) {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(((MetadataException) e).getErrorCode());
    } else {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(Status.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    LOGGER.warn(e.getMessage(), e);
    return responseResult;
  }
}
