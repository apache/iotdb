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
package org.apache.iotdb.db.qp.executor;

import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction;
import org.apache.iotdb.commons.udf.service.UDFManagementService;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.AuthorizerManager;
import org.apache.iotdb.db.engine.cache.BloomFilterCache;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.flush.pool.FlushTaskPoolManager;
import org.apache.iotdb.db.engine.trigger.service.TriggerRegistrationService;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.MNodeType;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator.AuthorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryIndexPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.UDAFPlan;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.CountPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowChildNodesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowChildPathsPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowNodesInTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPathsSetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPathsUsingTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPipePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.control.QueryTimeManager;
import org.apache.iotdb.db.query.dataset.AlignByDeviceDataSet;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.query.dataset.ShowDevicesDataSet;
import org.apache.iotdb.db.query.dataset.ShowTimeseriesDataSet;
import org.apache.iotdb.db.query.dataset.SingleDataSet;
import org.apache.iotdb.db.query.executor.IQueryRouter;
import org.apache.iotdb.db.query.executor.QueryRouter;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.sync.SyncService;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.EmptyDataSet;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadPoolExecutor;

import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_CHILD_NODES;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_CHILD_PATHS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_CHILD_PATHS_TYPES;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_COLUMN;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_COUNT;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_DEVICES;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_FUNCTION_CLASS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_FUNCTION_NAME;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_FUNCTION_TYPE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_ITEM;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_PIPE_CREATE_TIME;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_PIPE_MSG;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_PIPE_NAME;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_PIPE_REMOTE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_PIPE_ROLE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_PIPE_STATUS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_PRIVILEGE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_ROLE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_SCHEMA_TEMPLATE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_STORAGE_GROUP;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_DATATYPE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_ENCODING;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TTL;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_USER;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_VALUE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_BUILTIN_UDAF;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_BUILTIN_UDTF;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_EXTERNAL_UDAF;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_EXTERNAL_UDTF;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_NATIVE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.QUERY_ID;
import static org.apache.iotdb.commons.conf.IoTDBConstant.STATEMENT;

public class PlanExecutor implements IPlanExecutor {

  private static final Logger logger = LoggerFactory.getLogger(PlanExecutor.class);
  private static final Logger AUDIT_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.AUDIT_LOGGER_NAME);
  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");
  // for data query
  protected IQueryRouter queryRouter;
  // for administration
  private final AuthorizerManager authorizerManager;

  private ThreadPoolExecutor insertionPool;

  private static final String INSERT_MEASUREMENTS_FAILED_MESSAGE = "failed to insert measurements ";

  public PlanExecutor() throws QueryProcessException {
    queryRouter = new QueryRouter();
    authorizerManager = AuthorizerManager.getInstance();
  }

  @Override
  public QueryDataSet processQuery(PhysicalPlan queryPlan, QueryContext context)
      throws IOException, StorageEngineException, QueryFilterOptimizationException,
          QueryProcessException, MetadataException, InterruptedException {
    if (queryPlan instanceof QueryPlan) {
      return processDataQuery((QueryPlan) queryPlan, context);
    } else if (queryPlan instanceof AuthorPlan) {
      return processAuthorQuery((AuthorPlan) queryPlan);
    } else if (queryPlan instanceof ShowPlan) {
      return processShowQuery((ShowPlan) queryPlan, context);
    } else {
      throw new QueryProcessException(String.format("Unrecognized query plan %s", queryPlan));
    }
  }

  @Override
  public boolean processNonQuery(PhysicalPlan plan)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    return true;
  }

  public static void operateClearCache() {
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
  }

  protected QueryDataSet processDataQuery(QueryPlan queryPlan, QueryContext context)
      throws StorageEngineException, QueryFilterOptimizationException, QueryProcessException,
          IOException, InterruptedException {
    QueryDataSet queryDataSet;
    if (queryPlan instanceof AlignByDevicePlan) {
      queryDataSet = getAlignByDeviceDataSet((AlignByDevicePlan) queryPlan, context, queryRouter);
    } else {
      if (queryPlan.getPaths() == null || queryPlan.getPaths().isEmpty()) {
        // no time series are selected, return EmptyDataSet
        return new EmptyDataSet();
      } else if (queryPlan instanceof UDAFPlan) {
        UDAFPlan udafPlan = (UDAFPlan) queryPlan;
        queryDataSet = queryRouter.udafQuery(udafPlan, context);
      } else if (queryPlan instanceof UDTFPlan) {
        UDTFPlan udtfPlan = (UDTFPlan) queryPlan;
        queryDataSet = queryRouter.udtfQuery(udtfPlan, context);
      } else if (queryPlan instanceof GroupByTimeFillPlan) {
        GroupByTimeFillPlan groupByFillPlan = (GroupByTimeFillPlan) queryPlan;
        queryDataSet = queryRouter.groupByFill(groupByFillPlan, context);
      } else if (queryPlan instanceof GroupByTimePlan) {
        GroupByTimePlan groupByTimePlan = (GroupByTimePlan) queryPlan;
        queryDataSet = queryRouter.groupBy(groupByTimePlan, context);
      } else if (queryPlan instanceof QueryIndexPlan) {
        throw new QueryProcessException("Query index hasn't been supported yet");
      } else if (queryPlan instanceof AggregationPlan) {
        AggregationPlan aggregationPlan = (AggregationPlan) queryPlan;
        queryDataSet = queryRouter.aggregate(aggregationPlan, context);
      } else if (queryPlan instanceof FillQueryPlan) {
        FillQueryPlan fillQueryPlan = (FillQueryPlan) queryPlan;
        queryDataSet = queryRouter.fill(fillQueryPlan, context);
      } else if (queryPlan instanceof LastQueryPlan) {
        queryDataSet = queryRouter.lastQuery((LastQueryPlan) queryPlan, context);
      } else {
        queryDataSet = queryRouter.rawDataQuery((RawDataQueryPlan) queryPlan, context);
      }
    }
    queryDataSet.setRowLimit(queryPlan.getRowLimit());
    queryDataSet.setRowOffset(queryPlan.getRowOffset());
    queryDataSet.setWithoutAllNull(queryPlan.isWithoutAllNull());
    queryDataSet.setWithoutAnyNull(queryPlan.isWithoutAnyNull());
    queryDataSet.setWithoutNullColumnsIndex(queryPlan.getWithoutNullColumnsIndex());
    return queryDataSet;
  }

  protected AlignByDeviceDataSet getAlignByDeviceDataSet(
      AlignByDevicePlan plan, QueryContext context, IQueryRouter router)
      throws QueryProcessException {
    return new AlignByDeviceDataSet(plan, context, router);
  }

  protected QueryDataSet processShowQuery(ShowPlan showPlan, QueryContext context)
      throws QueryProcessException, MetadataException {
    switch (showPlan.getShowContentType()) {
      case TTL:
        return processShowTTLQuery((ShowTTLPlan) showPlan);
      case FLUSH_TASK_INFO:
        return processShowFlushTaskInfo();
      case VERSION:
        return processShowVersion();
      case TIMESERIES:
        return processShowTimeseries((ShowTimeSeriesPlan) showPlan, context);
      case STORAGE_GROUP:
        return processShowStorageGroup((ShowStorageGroupPlan) showPlan);
      case DEVICES:
        return processShowDevices((ShowDevicesPlan) showPlan);
      case CHILD_PATH:
        return processShowChildPaths((ShowChildPathsPlan) showPlan);
      case CHILD_NODE:
        return processShowChildNodes((ShowChildNodesPlan) showPlan);
      case COUNT_TIMESERIES:
        return processCountTimeSeries((CountPlan) showPlan);
      case COUNT_NODE_TIMESERIES:
        return processCountNodeTimeSeries((CountPlan) showPlan);
      case COUNT_DEVICES:
        return processCountDevices((CountPlan) showPlan);
      case COUNT_STORAGE_GROUP:
        return processCountStorageGroup((CountPlan) showPlan);
      case COUNT_NODES:
        return processCountNodes((CountPlan) showPlan);
      case QUERY_PROCESSLIST:
        return processShowQueryProcesslist();
      case FUNCTIONS:
        return processShowFunctions();
      case TRIGGERS:
        return processShowTriggers();
      case CONTINUOUS_QUERY:
        throw new UnsupportedOperationException();
      case SCHEMA_TEMPLATE:
        return processShowSchemaTemplates();
      case NODES_IN_SCHEMA_TEMPLATE:
        return processShowNodesInSchemaTemplate((ShowNodesInTemplatePlan) showPlan);
      case PATHS_SET_SCHEMA_TEMPLATE:
        return processShowPathsSetSchemaTemplate((ShowPathsSetTemplatePlan) showPlan);
      case PATHS_USING_SCHEMA_TEMPLATE:
        return processShowPathsUsingSchemaTemplate((ShowPathsUsingTemplatePlan) showPlan);
      case PIPE:
        return processShowPipes((ShowPipePlan) showPlan);
      default:
        throw new QueryProcessException(String.format("Unrecognized show plan %s", showPlan));
    }
  }

  private QueryDataSet processCountNodes(CountPlan countPlan) throws MetadataException {
    int num =
        getNodesNumInGivenLevel(
            countPlan.getPath(), countPlan.getLevel(), countPlan.isPrefixMatch());
    return createSingleDataSet(COLUMN_COUNT, TSDataType.INT32, num);
  }

  private QueryDataSet processCountNodeTimeSeries(CountPlan countPlan) throws MetadataException {
    Map<PartialPath, Integer> countResults = getTimeseriesCountGroupByLevel(countPlan);
    ListDataSet listDataSet =
        new ListDataSet(
            Arrays.asList(
                new PartialPath(COLUMN_COLUMN, false), new PartialPath(COLUMN_COUNT, false)),
            Arrays.asList(TSDataType.TEXT, TSDataType.INT32));
    for (PartialPath columnPath : countResults.keySet()) {
      RowRecord record = new RowRecord(0);
      Field field = new Field(TSDataType.TEXT);
      field.setBinaryV(new Binary(columnPath.getFullPath()));
      Field field1 = new Field(TSDataType.INT32);
      // get the count of every group
      field1.setIntV(countResults.get(columnPath));
      record.addField(field);
      record.addField(field1);
      listDataSet.putRecord(record);
    }
    return listDataSet;
  }

  private QueryDataSet processCountDevices(CountPlan countPlan) throws MetadataException {
    int num = getDevicesNum(countPlan.getPath(), countPlan.isPrefixMatch());
    return createSingleDataSet(COLUMN_DEVICES, TSDataType.INT32, num);
  }

  private QueryDataSet processCountStorageGroup(CountPlan countPlan) throws MetadataException {
    int num = getStorageGroupNum(countPlan.getPath(), countPlan.isPrefixMatch());
    return createSingleDataSet(COLUMN_STORAGE_GROUP, TSDataType.INT32, num);
  }

  private QueryDataSet createSingleDataSet(String columnName, TSDataType columnType, Object val) {
    SingleDataSet singleDataSet =
        new SingleDataSet(
            Collections.singletonList(new PartialPath(columnName, false)),
            Collections.singletonList(columnType));
    Field field = new Field(columnType);
    switch (columnType) {
      case TEXT:
        field.setBinaryV(((Binary) val));
        break;
      case FLOAT:
        field.setFloatV(((float) val));
        break;
      case INT32:
        field.setIntV(((int) val));
        break;
      case INT64:
        field.setLongV(((long) val));
        break;
      case DOUBLE:
        field.setDoubleV(((double) val));
        break;
      case BOOLEAN:
        field.setBoolV(((boolean) val));
        break;
      default:
        throw new UnSupportedDataTypeException("Unsupported data type" + columnType);
    }
    RowRecord record = new RowRecord(0);
    record.addField(field);
    singleDataSet.setRecord(record);
    return singleDataSet;
  }

  protected int getDevicesNum(PartialPath path, boolean isPrefixMatch) throws MetadataException {
    return IoTDB.schemaProcessor.getDevicesNum(path, isPrefixMatch);
  }

  private int getStorageGroupNum(PartialPath path, boolean isPrefixMatch) throws MetadataException {
    return IoTDB.schemaProcessor.getStorageGroupNum(path, isPrefixMatch);
  }

  protected int getPathsNum(PartialPath path, boolean isPrefixMatch) throws MetadataException {
    return IoTDB.schemaProcessor.getAllTimeseriesCount(path, isPrefixMatch);
  }

  protected int getNodesNumInGivenLevel(PartialPath path, int level, boolean isPrefixMatch)
      throws MetadataException {
    return IoTDB.schemaProcessor.getNodesCountInGivenLevel(path, level, isPrefixMatch);
  }

  protected List<MeasurementPath> getPathsName(PartialPath path) throws MetadataException {
    return IoTDB.schemaProcessor.getMeasurementPaths(path);
  }

  protected List<PartialPath> getNodesList(PartialPath schemaPattern, int level)
      throws MetadataException {
    return IoTDB.schemaProcessor.getNodesListInGivenLevel(schemaPattern, level);
  }

  private Map<PartialPath, Integer> getTimeseriesCountGroupByLevel(CountPlan countPlan)
      throws MetadataException {
    return IoTDB.schemaProcessor.getMeasurementCountGroupByLevel(
        countPlan.getPath(), countPlan.getLevel(), countPlan.isPrefixMatch());
  }

  private QueryDataSet processCountTimeSeries(CountPlan countPlan) throws MetadataException {
    int num = getPathsNum(countPlan.getPath(), countPlan.isPrefixMatch());
    return createSingleDataSet(COLUMN_COUNT, TSDataType.INT32, num);
  }

  private QueryDataSet processShowDevices(ShowDevicesPlan showDevicesPlan)
      throws MetadataException {
    return new ShowDevicesDataSet(showDevicesPlan);
  }

  private QueryDataSet processShowChildPaths(ShowChildPathsPlan showChildPathsPlan)
      throws MetadataException {
    Set<TSchemaNode> childPathsList = getPathNextChildren(showChildPathsPlan.getPath());

    // sort by node type
    Set<TSchemaNode> sortSet =
        new TreeSet<>(
            (o1, o2) -> {
              if (o1.getNodeType() == o2.getNodeType()) {
                return o1.getNodeName().compareTo(o2.getNodeName());
              }
              return o1.getNodeType() - o2.getNodeType();
            });
    sortSet.addAll(childPathsList);
    ListDataSet listDataSet =
        new ListDataSet(
            Arrays.asList(
                new PartialPath(COLUMN_CHILD_PATHS, false),
                new PartialPath(COLUMN_CHILD_PATHS_TYPES, false)),
            Arrays.asList(TSDataType.TEXT, TSDataType.TEXT));
    for (TSchemaNode node : sortSet) {
      RowRecord record = new RowRecord(0);
      Field field = new Field(TSDataType.TEXT);
      field.setBinaryV(new Binary(node.getNodeName()));
      record.addField(field);
      field = new Field(TSDataType.TEXT);
      field.setBinaryV(new Binary(MNodeType.getMNodeType(node.getNodeType()).getNodeTypeName()));
      record.addField(field);
      listDataSet.putRecord(record);
    }
    return listDataSet;
  }

  protected Set<TSchemaNode> getPathNextChildren(PartialPath path) throws MetadataException {
    return IoTDB.schemaProcessor.getChildNodePathInNextLevel(path);
  }

  private QueryDataSet processShowChildNodes(ShowChildNodesPlan showChildNodesPlan)
      throws MetadataException {
    // getNodeNextChildren
    Set<String> childNodesList = getNodeNextChildren(showChildNodesPlan.getPath());
    ListDataSet listDataSet =
        new ListDataSet(
            Collections.singletonList(new PartialPath(COLUMN_CHILD_NODES, false)),
            Collections.singletonList(TSDataType.TEXT));
    for (String s : childNodesList) {
      RowRecord record = new RowRecord(0);
      Field field = new Field(TSDataType.TEXT);
      field.setBinaryV(new Binary(s));
      record.addField(field);
      listDataSet.putRecord(record);
    }
    return listDataSet;
  }

  protected Set<String> getNodeNextChildren(PartialPath path) throws MetadataException {
    return IoTDB.schemaProcessor.getChildNodeNameInNextLevel(path);
  }

  protected List<PartialPath> getStorageGroupNames(PartialPath path, boolean isPrefixMatch)
      throws MetadataException {
    return IoTDB.schemaProcessor.getMatchedStorageGroups(path, isPrefixMatch);
  }

  private QueryDataSet processShowStorageGroup(ShowStorageGroupPlan showStorageGroupPlan)
      throws MetadataException {
    ListDataSet listDataSet =
        new ListDataSet(
            Collections.singletonList(new PartialPath(COLUMN_STORAGE_GROUP, false)),
            Collections.singletonList(TSDataType.TEXT));
    List<PartialPath> storageGroupList =
        getStorageGroupNames(showStorageGroupPlan.getPath(), showStorageGroupPlan.isPrefixMatch());
    addToDataSet(storageGroupList, listDataSet);
    return listDataSet;
  }

  private void addToDataSet(Collection<PartialPath> paths, ListDataSet dataSet) {
    for (PartialPath s : paths) {
      RowRecord record = new RowRecord(0);
      Field field = new Field(TSDataType.TEXT);
      field.setBinaryV(new Binary(s.getFullPath()));
      record.addField(field);
      dataSet.putRecord(record);
    }
  }

  private void addLockInfoToDataSet(
      List<PartialPath> paths, List<String> lockHolderList, ListDataSet dataSet) {
    for (int i = 0; i < paths.size(); i++) {
      RowRecord record = new RowRecord(0);
      Field field = new Field(TSDataType.TEXT);
      field.setBinaryV(new Binary(paths.get(i).getFullPath()));
      record.addField(field);
      field = new Field(TSDataType.TEXT);
      field.setBinaryV(new Binary(lockHolderList.get(i)));
      record.addField(field);
      dataSet.putRecord(record);
    }
  }

  private QueryDataSet processShowTimeseries(
      ShowTimeSeriesPlan showTimeSeriesPlan, QueryContext context) throws MetadataException {
    return new ShowTimeseriesDataSet(showTimeSeriesPlan, context);
  }

  protected List<IStorageGroupMNode> getAllStorageGroupNodes() {
    return IoTDB.schemaProcessor.getAllStorageGroupNodes();
  }

  private QueryDataSet processShowTTLQuery(ShowTTLPlan showTTLPlan) {
    ListDataSet listDataSet =
        new ListDataSet(
            Arrays.asList(
                new PartialPath(COLUMN_STORAGE_GROUP, false), new PartialPath(COLUMN_TTL, false)),
            Arrays.asList(TSDataType.TEXT, TSDataType.INT64));
    Set<PartialPath> selectedSgs = new HashSet<>(showTTLPlan.getStorageGroups());

    List<IStorageGroupMNode> storageGroups = getAllStorageGroupNodes();
    int timestamp = 0;
    for (IStorageGroupMNode mNode : storageGroups) {
      PartialPath sgName = mNode.getPartialPath();
      if (!selectedSgs.isEmpty() && !selectedSgs.contains(sgName)) {
        continue;
      }
      RowRecord rowRecord = new RowRecord(timestamp++);
      Field sg = new Field(TSDataType.TEXT);
      Field ttl;
      sg.setBinaryV(new Binary(sgName.getFullPath()));
      if (mNode.getDataTTL() != Long.MAX_VALUE) {
        ttl = new Field(TSDataType.INT64);
        ttl.setLongV(mNode.getDataTTL());
      } else {
        ttl = null;
      }
      rowRecord.addField(sg);
      rowRecord.addField(ttl);
      listDataSet.putRecord(rowRecord);
    }

    return listDataSet;
  }

  private QueryDataSet processShowVersion() {
    SingleDataSet singleDataSet =
        new SingleDataSet(
            Arrays.asList(
                new PartialPath(IoTDBConstant.COLUMN_VERSION, false),
                new PartialPath(IoTDBConstant.COLUMN_BUILD_INFO, false)),
            Arrays.asList(TSDataType.TEXT, TSDataType.TEXT));
    Field field = new Field(TSDataType.TEXT);
    field.setBinaryV(new Binary(IoTDBConstant.VERSION));
    RowRecord rowRecord = new RowRecord(0);
    rowRecord.addField(field);
    field = new Field(TSDataType.TEXT);
    field.setBinaryV(new Binary(IoTDBConstant.BUILD_INFO));
    rowRecord.addField(field);
    singleDataSet.setRecord(rowRecord);
    return singleDataSet;
  }

  private QueryDataSet processShowFlushTaskInfo() {
    ListDataSet listDataSet =
        new ListDataSet(
            Arrays.asList(
                new PartialPath(COLUMN_ITEM, false), new PartialPath(COLUMN_VALUE, false)),
            Arrays.asList(TSDataType.TEXT, TSDataType.TEXT));

    int timestamp = 0;
    addRowRecordForShowQuery(
        listDataSet,
        timestamp++,
        "total number of flush tasks",
        Integer.toString(FlushTaskPoolManager.getInstance().getTotalTasks()));
    addRowRecordForShowQuery(
        listDataSet,
        timestamp++,
        "number of working flush tasks",
        Integer.toString(FlushTaskPoolManager.getInstance().getWorkingTasksNumber()));
    addRowRecordForShowQuery(
        listDataSet,
        timestamp,
        "number of waiting flush tasks",
        Integer.toString(FlushTaskPoolManager.getInstance().getWaitingTasksNumber()));
    return listDataSet;
  }

  private QueryDataSet processShowFunctions() throws QueryProcessException {
    ListDataSet listDataSet =
        new ListDataSet(
            Arrays.asList(
                new PartialPath(COLUMN_FUNCTION_NAME, false),
                new PartialPath(COLUMN_FUNCTION_TYPE, false),
                new PartialPath(COLUMN_FUNCTION_CLASS, false)),
            Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT));

    appendUDFs(listDataSet);
    appendNativeFunctions(listDataSet);

    listDataSet.sort(
        (r1, r2) ->
            String.CASE_INSENSITIVE_ORDER.compare(
                r1.getFields().get(0).getStringValue(), r2.getFields().get(0).getStringValue()));
    return listDataSet;
  }

  private void appendUDFs(ListDataSet listDataSet) throws QueryProcessException {
    for (UDFInformation info : UDFManagementService.getInstance().getAllUDFInformation()) {
      RowRecord rowRecord = new RowRecord(0); // ignore timestamp
      rowRecord.addField(Binary.valueOf(info.getFunctionName()), TSDataType.TEXT);
      String functionType = "";
      try {
        if (info.isBuiltin()) {
          if (UDFManagementService.getInstance().isUDTF(info.getFunctionName())) {
            functionType = FUNCTION_TYPE_BUILTIN_UDTF;
          } else if (UDFManagementService.getInstance().isUDAF(info.getFunctionName())) {
            functionType = FUNCTION_TYPE_BUILTIN_UDAF;
          }
        } else {
          if (UDFManagementService.getInstance().isUDTF(info.getFunctionName())) {
            functionType = FUNCTION_TYPE_EXTERNAL_UDTF;
          } else if (UDFManagementService.getInstance().isUDAF(info.getFunctionName())) {
            functionType = FUNCTION_TYPE_EXTERNAL_UDAF;
          }
        }
      } catch (InstantiationException
          | InvocationTargetException
          | NoSuchMethodException
          | IllegalAccessException e) {
        throw new QueryProcessException(e.toString());
      }
      rowRecord.addField(Binary.valueOf(functionType), TSDataType.TEXT);
      rowRecord.addField(Binary.valueOf(info.getClassName()), TSDataType.TEXT);
      listDataSet.putRecord(rowRecord);
    }
  }

  private QueryDataSet processShowSchemaTemplates() {
    ListDataSet listDataSet =
        new ListDataSet(
            Collections.singletonList(new PartialPath(COLUMN_SCHEMA_TEMPLATE, false)),
            Collections.singletonList(TSDataType.TEXT));
    Set<String> allTemplates = IoTDB.schemaProcessor.getAllTemplates();
    for (String templateName : allTemplates) {
      RowRecord rowRecord = new RowRecord(0); // ignore timestamp
      rowRecord.addField(Binary.valueOf(templateName), TSDataType.TEXT);
      listDataSet.putRecord(rowRecord);
    }
    return listDataSet;
  }

  private QueryDataSet processShowNodesInSchemaTemplate(ShowNodesInTemplatePlan showPlan)
      throws QueryProcessException {
    ListDataSet listDataSet =
        new ListDataSet(
            Arrays.asList(
                new PartialPath(COLUMN_CHILD_NODES, false),
                new PartialPath(COLUMN_TIMESERIES_DATATYPE, false),
                new PartialPath(COLUMN_TIMESERIES_ENCODING, false),
                new PartialPath(COLUMN_TIMESERIES_COMPRESSION, false)),
            Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT));
    try {
      List<Pair<String, IMeasurementSchema>> measurements =
          IoTDB.schemaProcessor.getSchemasInTemplate(showPlan.getTemplateName(), "");
      for (Pair<String, IMeasurementSchema> measurement : measurements) {
        RowRecord rowRecord = new RowRecord(0); // ignore timestamp
        rowRecord.addField(Binary.valueOf(measurement.left), TSDataType.TEXT);

        IMeasurementSchema measurementSchema = measurement.right;
        rowRecord.addField(Binary.valueOf(measurementSchema.getType().toString()), TSDataType.TEXT);
        rowRecord.addField(
            Binary.valueOf(measurementSchema.getEncodingType().toString()), TSDataType.TEXT);
        rowRecord.addField(
            Binary.valueOf(measurementSchema.getCompressor().toString()), TSDataType.TEXT);
        listDataSet.putRecord(rowRecord);
      }
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
    return listDataSet;
  }

  private QueryDataSet processShowPathsUsingSchemaTemplate(ShowPathsUsingTemplatePlan showPlan)
      throws QueryProcessException {
    ListDataSet listDataSet =
        new ListDataSet(
            Collections.singletonList(new PartialPath(COLUMN_CHILD_PATHS, false)),
            Collections.singletonList(TSDataType.TEXT));
    try {
      Set<String> paths = IoTDB.schemaProcessor.getPathsUsingTemplate(showPlan.getTemplateName());
      for (String path : paths) {
        RowRecord rowRecord = new RowRecord(0); // ignore timestamp
        rowRecord.addField(Binary.valueOf(path), TSDataType.TEXT);
        listDataSet.putRecord(rowRecord);
      }
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
    return listDataSet;
  }

  private QueryDataSet processShowPathsSetSchemaTemplate(ShowPathsSetTemplatePlan showPlan)
      throws QueryProcessException {
    ListDataSet listDataSet =
        new ListDataSet(
            Collections.singletonList(new PartialPath(COLUMN_CHILD_PATHS, false)),
            Collections.singletonList(TSDataType.TEXT));
    try {
      Set<String> paths = IoTDB.schemaProcessor.getPathsSetTemplate(showPlan.getTemplateName());
      for (String path : paths) {
        RowRecord rowRecord = new RowRecord(0); // ignore timestamp
        rowRecord.addField(Binary.valueOf(path), TSDataType.TEXT);
        listDataSet.putRecord(rowRecord);
      }
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
    return listDataSet;
  }

  private void appendNativeFunctions(ListDataSet listDataSet) {
    final Binary functionType = Binary.valueOf(FUNCTION_TYPE_NATIVE);
    final Binary className = Binary.valueOf("");
    for (String functionName : BuiltinAggregationFunction.getNativeFunctionNames()) {
      RowRecord rowRecord = new RowRecord(0); // ignore timestamp
      rowRecord.addField(Binary.valueOf(functionName.toUpperCase()), TSDataType.TEXT);
      rowRecord.addField(functionType, TSDataType.TEXT);
      rowRecord.addField(className, TSDataType.TEXT);
      listDataSet.putRecord(rowRecord);
    }
  }

  private QueryDataSet processShowTriggers() {
    return TriggerRegistrationService.getInstance().show();
  }

  private void addRowRecordForShowQuery(
      ListDataSet listDataSet, int timestamp, String item, String value) {
    RowRecord rowRecord = new RowRecord(timestamp);
    Field itemField = new Field(TSDataType.TEXT);
    itemField.setBinaryV(new Binary(item));
    Field valueField = new Field(TSDataType.TEXT);
    valueField.setBinaryV(new Binary(value));
    rowRecord.addField(itemField);
    rowRecord.addField(valueField);
    listDataSet.putRecord(rowRecord);
  }

  private QueryDataSet processShowPipes(ShowPipePlan plan) {
    ListDataSet listDataSet =
        new ListDataSet(
            Arrays.asList(
                new PartialPath(COLUMN_PIPE_CREATE_TIME, false),
                new PartialPath(COLUMN_PIPE_NAME, false),
                new PartialPath(COLUMN_PIPE_ROLE, false),
                new PartialPath(COLUMN_PIPE_REMOTE, false),
                new PartialPath(COLUMN_PIPE_STATUS, false),
                new PartialPath(COLUMN_PIPE_MSG, false)),
            Arrays.asList(
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT));
    SyncService.getInstance().showPipe(plan, listDataSet);
    // sort by create time
    listDataSet.sort(Comparator.comparing(o -> o.getFields().get(0).getStringValue()));
    return listDataSet;
  }

  // high Cognitive Complexity

  protected QueryDataSet processAuthorQuery(AuthorPlan plan) throws QueryProcessException {
    AuthorType authorType = plan.getAuthorType();
    String userName = plan.getUserName();
    String roleName = plan.getRoleName();
    List<PartialPath> nodeNameList = plan.getNodeNameList();

    ListDataSet dataSet;

    try {
      switch (authorType) {
        case LIST_ROLE:
          if (userName != null) {
            dataSet = executeListUserRoles(userName);
          } else {
            dataSet = executeListRole(plan);
          }
          break;
        case LIST_USER:
          if (roleName != null) {
            dataSet = executeListRoleUsers(roleName);
          } else {
            dataSet = executeListUser(plan);
          }
          break;
        case LIST_ROLE_PRIVILEGE:
          dataSet = executeListRolePrivileges(roleName, nodeNameList);
          break;
        case LIST_USER_PRIVILEGE:
          dataSet = executeListUserPrivileges(userName, nodeNameList);
          break;
        default:
          throw new QueryProcessException("Unsupported operation " + authorType);
      }
    } catch (AuthException e) {
      throw new QueryProcessException(e.getMessage());
    }
    return dataSet;
  }

  private ListDataSet executeListRole(AuthorPlan plan) throws AuthException {
    ListDataSet dataSet =
        new ListDataSet(
            Collections.singletonList(new PartialPath(COLUMN_ROLE, false)),
            Collections.singletonList(TSDataType.TEXT));

    // check if current user is granted list_role privilege
    boolean hasListRolePrivilege =
        AuthorityChecker.check(
            plan.getLoginUserName(),
            Collections.emptyList(),
            plan.getOperatorType(),
            plan.getLoginUserName());
    if (!hasListRolePrivilege) {
      return dataSet;
    }

    List<String> roleList = authorizerManager.listAllRoles();
    addToDataSet(roleList, dataSet);
    return dataSet;
  }

  private void addToDataSet(List<String> strResults, ListDataSet dataSet) {
    int index = 0;
    for (String role : strResults) {
      RowRecord record = new RowRecord(index++);
      Field field = new Field(TSDataType.TEXT);
      field.setBinaryV(new Binary(role));
      record.addField(field);
      dataSet.putRecord(record);
    }
  }

  private ListDataSet executeListUser(AuthorPlan plan) throws AuthException {
    ListDataSet dataSet =
        new ListDataSet(
            Collections.singletonList(new PartialPath(COLUMN_USER, false)),
            Collections.singletonList(TSDataType.TEXT));

    // check if current user is granted list_user privilege
    boolean hasListUserPrivilege =
        AuthorityChecker.check(
            plan.getLoginUserName(),
            plan.getNodeNameList(),
            plan.getOperatorType(),
            plan.getLoginUserName());
    if (!hasListUserPrivilege) {
      return dataSet;
    }

    List<String> userList = authorizerManager.listAllUsers();
    addToDataSet(userList, dataSet);
    return dataSet;
  }

  private ListDataSet executeListRoleUsers(String roleName) throws AuthException {
    Role role = authorizerManager.getRole(roleName);
    if (role == null) {
      throw new AuthException("No such role : " + roleName);
    }
    ListDataSet dataSet =
        new ListDataSet(
            Collections.singletonList(new PartialPath(COLUMN_USER, false)),
            Collections.singletonList(TSDataType.TEXT));
    List<String> userList = authorizerManager.listAllUsers();
    int index = 0;
    for (String userN : userList) {
      User userObj = authorizerManager.getUser(userN);
      if (userObj != null && userObj.hasRole(roleName)) {
        RowRecord record = new RowRecord(index++);
        Field field = new Field(TSDataType.TEXT);
        field.setBinaryV(new Binary(userN));
        record.addField(field);
        dataSet.putRecord(record);
      }
    }
    return dataSet;
  }

  private ListDataSet executeListUserRoles(String userName) throws AuthException {
    User user = authorizerManager.getUser(userName);
    if (user != null) {
      ListDataSet dataSet =
          new ListDataSet(
              Collections.singletonList(new PartialPath(COLUMN_ROLE, false)),
              Collections.singletonList(TSDataType.TEXT));
      int index = 0;
      for (String roleN : user.getRoleList()) {
        RowRecord record = new RowRecord(index++);
        Field field = new Field(TSDataType.TEXT);
        field.setBinaryV(new Binary(roleN));
        record.addField(field);
        dataSet.putRecord(record);
      }
      return dataSet;
    } else {
      throw new AuthException("No such user : " + userName);
    }
  }

  private ListDataSet executeListRolePrivileges(String roleName, List<PartialPath> nodeNameList)
      throws AuthException {
    Role role = authorizerManager.getRole(roleName);
    if (role != null) {
      List<PartialPath> headerList = new ArrayList<>();
      List<TSDataType> typeList = new ArrayList<>();
      headerList.add(new PartialPath(COLUMN_PRIVILEGE, false));
      typeList.add(TSDataType.TEXT);
      ListDataSet dataSet = new ListDataSet(headerList, typeList);
      int index = 0;
      for (PathPrivilege pathPrivilege : role.getPrivilegeList()) {
        if (nodeNameList.isEmpty()) {
          RowRecord record = new RowRecord(index++);
          Field field = new Field(TSDataType.TEXT);
          field.setBinaryV(new Binary(pathPrivilege.toString()));
          record.addField(field);
          dataSet.putRecord(record);
          continue;
        }
        for (PartialPath path : nodeNameList) {
          if (AuthUtils.pathBelongsTo(pathPrivilege.getPath(), path.getFullPath())) {
            RowRecord record = new RowRecord(index++);
            Field field = new Field(TSDataType.TEXT);
            field.setBinaryV(new Binary(pathPrivilege.toString()));
            record.addField(field);
            dataSet.putRecord(record);
          }
        }
      }
      return dataSet;
    } else {
      throw new AuthException("No such role : " + roleName);
    }
  }

  private ListDataSet executeListUserPrivileges(String userName, List<PartialPath> nodeNameList)
      throws AuthException {
    User user = authorizerManager.getUser(userName);
    if (user == null) {
      throw new AuthException("No such user : " + userName);
    }
    List<PartialPath> headerList = new ArrayList<>();
    List<TSDataType> typeList = new ArrayList<>();
    int index = 0;
    if (CommonDescriptor.getInstance().getConfig().getAdminName().equals(userName)) {
      headerList.add(new PartialPath(COLUMN_PRIVILEGE, false));
      typeList.add(TSDataType.TEXT);
      ListDataSet dataSet = new ListDataSet(headerList, typeList);
      for (PrivilegeType privilegeType : PrivilegeType.values()) {
        RowRecord record = new RowRecord(index++);
        Field privilegeF = new Field(TSDataType.TEXT);
        privilegeF.setBinaryV(new Binary(privilegeType.toString()));
        record.addField(privilegeF);
        dataSet.putRecord(record);
      }
      return dataSet;
    } else {
      headerList.add(new PartialPath(COLUMN_ROLE, false));
      headerList.add(new PartialPath(COLUMN_PRIVILEGE, false));
      typeList.add(TSDataType.TEXT);
      typeList.add(TSDataType.TEXT);
      ListDataSet dataSet = new ListDataSet(headerList, typeList);
      for (PathPrivilege pathPrivilege : user.getPrivilegeList()) {
        if (nodeNameList.isEmpty()) {
          RowRecord record = new RowRecord(index++);
          Field roleF = new Field(TSDataType.TEXT);
          roleF.setBinaryV(new Binary(""));
          record.addField(roleF);
          Field privilegeF = new Field(TSDataType.TEXT);
          privilegeF.setBinaryV(new Binary(pathPrivilege.toString()));
          record.addField(privilegeF);
          dataSet.putRecord(record);
          continue;
        }
        for (PartialPath path : nodeNameList) {
          if (AuthUtils.pathBelongsTo(pathPrivilege.getPath(), path.getFullPath())) {
            RowRecord record = new RowRecord(index++);
            Field roleF = new Field(TSDataType.TEXT);
            roleF.setBinaryV(new Binary(""));
            record.addField(roleF);
            Field privilegeF = new Field(TSDataType.TEXT);
            privilegeF.setBinaryV(new Binary(pathPrivilege.toString()));
            record.addField(privilegeF);
            dataSet.putRecord(record);
          }
        }
      }
      for (String roleN : user.getRoleList()) {
        Role role = authorizerManager.getRole(roleN);
        if (role == null) {
          continue;
        }
        for (PathPrivilege pathPrivilege : role.getPrivilegeList()) {
          if (nodeNameList.isEmpty()) {
            RowRecord record = new RowRecord(index++);
            Field roleF = new Field(TSDataType.TEXT);
            roleF.setBinaryV(new Binary(roleN));
            record.addField(roleF);
            Field privilegeF = new Field(TSDataType.TEXT);
            privilegeF.setBinaryV(new Binary(pathPrivilege.toString()));
            record.addField(privilegeF);
            dataSet.putRecord(record);
          }
          for (PartialPath path : nodeNameList) {
            if (AuthUtils.pathBelongsTo(pathPrivilege.getPath(), path.getFullPath())) {
              RowRecord record = new RowRecord(index++);
              Field roleF = new Field(TSDataType.TEXT);
              roleF.setBinaryV(new Binary(roleN));
              record.addField(roleF);
              Field privilegeF = new Field(TSDataType.TEXT);
              privilegeF.setBinaryV(new Binary(pathPrivilege.toString()));
              record.addField(privilegeF);
              dataSet.putRecord(record);
            }
          }
        }
      }
      return dataSet;
    }
  }

  private boolean processShowQueryResource() {
    DEBUG_LOGGER.info(String.format("**********%s**********\n\n", new Date()));
    FileReaderManager.getInstance().writeFileReferenceInfo();
    QueryResourceManager.getInstance().writeQueryFileInfo();
    DEBUG_LOGGER.info("\n****************************************************\n\n");
    return true;
  }

  private QueryDataSet processShowQueryProcesslist() {
    ListDataSet listDataSet =
        new ListDataSet(
            Arrays.asList(new PartialPath(QUERY_ID, false), new PartialPath(STATEMENT, false)),
            Arrays.asList(TSDataType.INT64, TSDataType.TEXT));
    QueryTimeManager queryTimeManager = QueryTimeManager.getInstance();
    for (Entry<Long, QueryContext> context : queryTimeManager.getQueryContextMap().entrySet()) {
      RowRecord record = new RowRecord(context.getValue().getStartTime());
      record.addField(context.getKey(), TSDataType.INT64);
      record.addField(new Binary(context.getValue().getStatement()), TSDataType.TEXT);
      listDataSet.putRecord(record);
    }
    return listDataSet;
  }
}
