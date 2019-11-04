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

import static org.apache.iotdb.db.conf.IoTDBConstant.PRIVILEGE;
import static org.apache.iotdb.db.conf.IoTDBConstant.ROLE;
import static org.apache.iotdb.db.conf.IoTDBConstant.USER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.auth.entity.PathPrivilege;
import org.apache.iotdb.db.auth.entity.Role;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageGroupException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MNode;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator.AuthorType;
import org.apache.iotdb.db.qp.logical.sys.PropertyOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.DataAuthPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.PropertyPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.db.utils.AuthUtils;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.cache.CacheException;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;

public class QueryProcessExecutor extends AbstractQueryProcessExecutor {

  private StorageEngine storageEngine;
  private MManager mManager = MManager.getInstance();

  public QueryProcessExecutor() {
    storageEngine = StorageEngine.getInstance();
  }

  @Override
  public boolean processNonQuery(PhysicalPlan plan) throws ProcessorException {
    switch (plan.getOperatorType()) {
      case DELETE:
        return delete((DeletePlan) plan);
      case UPDATE:
        UpdatePlan update = (UpdatePlan) plan;
        boolean flag = true;
        for (Pair<Long, Long> timePair : update.getIntervals()) {
          flag &= update(update.getPath(), timePair.left, timePair.right, update.getValue());
        }
        return flag;
      case INSERT:
        return insert((InsertPlan) plan);
      case CREATE_ROLE:
      case DELETE_ROLE:
      case CREATE_USER:
      case REVOKE_USER_ROLE:
      case REVOKE_ROLE_PRIVILEGE:
      case REVOKE_USER_PRIVILEGE:
      case GRANT_ROLE_PRIVILEGE:
      case GRANT_USER_PRIVILEGE:
      case GRANT_USER_ROLE:
      case MODIFY_PASSWORD:
      case DELETE_USER:
        AuthorPlan author = (AuthorPlan) plan;
        return operateAuthor(author);
      case GRANT_WATERMARK_EMBEDDING:
        return operateWatermarkEmbedding(((DataAuthPlan) plan).getUsers(), true);
      case REVOKE_WATERMARK_EMBEDDING:
        return operateWatermarkEmbedding(((DataAuthPlan) plan).getUsers(), false);
      case DELETE_TIMESERIES:
        return deleteTimeSeries((DeleteTimeSeriesPlan) plan);
      case CREATE_TIMESERIES:
        return createTimeSeries((CreateTimeSeriesPlan) plan);
      case SET_STORAGE_GROUP:
        return setStorageGroup((SetStorageGroupPlan) plan);
      case DELETE_STORAGE_GROUP:
        return deleteStorageGroup((DeleteStorageGroupPlan) plan);  
      case PROPERTY:
        PropertyPlan property = (PropertyPlan) plan;
        return operateProperty(property);
      case TTL:
        operateTTL((SetTTLPlan) plan);
        return true;
      default:
        throw new UnsupportedOperationException(
            String.format("operation %s is not supported", plan.getOperatorType()));
    }
  }

  private void operateTTL(SetTTLPlan plan) throws ProcessorException {
    try {
      MManager.getInstance().setTTL(plan.getStorageGroup(), plan.getDataTTL());
      StorageEngine.getInstance().setTTL(plan.getStorageGroup(), plan.getDataTTL());
    } catch (PathErrorException | IOException | StorageEngineException e) {
      throw new ProcessorException(e);
    }
  }

  @Override
  public TSDataType getSeriesType(Path path) throws PathErrorException {
    if (path.equals(SQLConstant.RESERVED_TIME)) {
      return TSDataType.INT64;
    }
    if (path.equals(SQLConstant.RESERVED_FREQ)) {
      return TSDataType.FLOAT;
    }
    return MManager.getInstance().getSeriesType(path.getFullPath());
  }

  @Override
  public boolean judgePathExists(Path path) {
    if (SQLConstant.isReservedPath(path)) {
      return true;
    }
    return MManager.getInstance().pathExist(path.getFullPath());
  }

  @Override
  public QueryDataSet aggregate(List<Path> paths, List<String> aggres, IExpression expression,
      QueryContext context)
      throws ProcessorException, StorageEngineException, QueryFilterOptimizationException,
      PathErrorException, IOException {
    return queryRouter.aggregate(paths, aggres, expression, context);
  }

  @Override
  public QueryDataSet fill(List<Path> fillPaths, long queryTime, Map<TSDataType, IFill> fillTypes,
      QueryContext context)
      throws IOException, PathErrorException, StorageEngineException {
    return queryRouter.fill(fillPaths, queryTime, fillTypes, context);
  }

  @Override
  public QueryDataSet groupBy(List<Path> paths, List<String> aggres, IExpression expression,
      long unit, long origin, List<Pair<Long, Long>> intervals, QueryContext context)
      throws ProcessorException, StorageEngineException, QueryFilterOptimizationException,
      PathErrorException, IOException {
    return queryRouter.groupBy(paths, aggres, expression, unit, origin, intervals, context);
  }

  @Override
  public boolean update(Path path, long startTime, long endTime, String value)
      throws ProcessorException {
    return false;
  }

  @Override
  public boolean delete(Path path, long timestamp) throws ProcessorException {
    String deviceId = path.getDevice();
    String measurementId = path.getMeasurement();
    try {
      if (!mManager.pathExist(path.getFullPath())) {
        throw new ProcessorException(
            String.format("Time series %s does not exist.", path.getFullPath()));
      }
      mManager.getStorageGroupNameByPath(path.getFullPath());
      storageEngine.delete(deviceId, measurementId, timestamp);
      return true;
    } catch (StorageGroupException | StorageEngineException e) {
      throw new ProcessorException(e);
    }
  }


  @Override
  public boolean insert(InsertPlan insertPlan) throws ProcessorException {
    try {
      String[] measurementList = insertPlan.getMeasurements();
      String deviceId = insertPlan.getDeviceId();
      MNode node = mManager.getNodeByDeviceIdFromCache(deviceId);
      String[] strValues = insertPlan.getValues();
      TSDataType[] dataTypes = new TSDataType[measurementList.length];
      IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();

      for (int i = 0; i < measurementList.length; i++) {

        // check if timeseries exists
        if (!node.hasChild(measurementList[i])) {
          if (!conf.isAutoCreateSchemaEnabled()) {
            throw new ProcessorException(
                String.format("Current deviceId[%s] does not contain measurement:%s",
                    deviceId, measurementList[i]));
          }
          try {
            addPathToMTree(deviceId, measurementList[i], strValues[i]);
          } catch (MetadataErrorException e) {
            if (!e.getMessage().contains("already exist")) {
              throw e;
            }
          }
        }
        MNode measurementNode = node.getChild(measurementList[i]);
        if (!measurementNode.isLeaf()) {
          throw new ProcessorException(
              String.format("Current Path is not leaf node. %s.%s", deviceId,
                  measurementList[i]));
        }

        dataTypes[i] = measurementNode.getSchema().getType();
      }
      insertPlan.setDataTypes(dataTypes);
      return storageEngine.insert(insertPlan);

    } catch (PathErrorException | StorageEngineException | MetadataErrorException | CacheException e) {
      throw new ProcessorException(e);
    }
  }

  @Override
  public Integer[] insertBatch(BatchInsertPlan batchInsertPlan) throws ProcessorException {
    try {
      String[] measurementList = batchInsertPlan.getMeasurements();
      String deviceId = batchInsertPlan.getDeviceId();
      MNode node = mManager.getNodeByDeviceIdFromCache(deviceId);
      TSDataType[] dataTypes = batchInsertPlan.getDataTypes();
      IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();

      for (int i = 0; i < measurementList.length; i++) {

        // check if timeseries exists
        if (!node.hasChild(measurementList[i])) {
          if (!conf.isAutoCreateSchemaEnabled()) {
            throw new ProcessorException(
                String.format("Current deviceId[%s] does not contain measurement:%s",
                    deviceId, measurementList[i]));
          }
          addPathToMTree(deviceId, measurementList[i], dataTypes[i]);
        }
        MNode measurementNode = node.getChild(measurementList[i]);
        if (!measurementNode.isLeaf()) {
          throw new ProcessorException(
              String.format("Current Path is not leaf node. %s.%s", deviceId, measurementList[i]));
        }

        // check data type
        if (measurementNode.getSchema().getType() != batchInsertPlan.getDataTypes()[i]) {
          throw new ProcessorException(String
              .format("Datatype mismatch, Insert measurement %s type %s, metadata tree type %s",
                  measurementList[i], batchInsertPlan.getDataTypes()[i],
                  measurementNode.getSchema().getType()));
        }
      }
      return storageEngine.insertBatch(batchInsertPlan);

    } catch (PathErrorException | StorageEngineException | MetadataErrorException | CacheException e) {
      throw new ProcessorException(e);
    }
  }

  @Override
  public List<String> getAllPaths(String originPath) throws MetadataErrorException {
    return MManager.getInstance().getPaths(originPath);
  }

  private boolean operateAuthor(AuthorPlan author) throws ProcessorException {
    AuthorOperator.AuthorType authorType = author.getAuthorType();
    String userName = author.getUserName();
    String roleName = author.getRoleName();
    String password = author.getPassword();
    String newPassword = author.getNewPassword();
    Set<Integer> permissions = author.getPermissions();
    Path nodeName = author.getNodeName();
    IAuthorizer authorizer;
    try {
      authorizer = LocalFileAuthorizer.getInstance();
    } catch (AuthException e) {
      throw new ProcessorException(e);
    }
    try {
      switch (authorType) {
        case UPDATE_USER:
          authorizer.updateUserPassword(userName, newPassword);
          break;
        case CREATE_USER:
          authorizer.createUser(userName, password);
          break;
        case CREATE_ROLE:
          authorizer.createRole(roleName);
          break;
        case DROP_USER:
          authorizer.deleteUser(userName);
          break;
        case DROP_ROLE:
          authorizer.deleteRole(roleName);
          break;
        case GRANT_ROLE:
          for (int i : permissions) {
            authorizer.grantPrivilegeToRole(roleName, nodeName.getFullPath(), i);
          }
          break;
        case GRANT_USER:
          for (int i : permissions) {
            authorizer.grantPrivilegeToUser(userName, nodeName.getFullPath(), i);
          }
          break;
        case GRANT_ROLE_TO_USER:
          authorizer.grantRoleToUser(roleName, userName);
          break;
        case REVOKE_USER:
          for (int i : permissions) {
            authorizer.revokePrivilegeFromUser(userName, nodeName.getFullPath(), i);
          }
          break;
        case REVOKE_ROLE:
          for (int i : permissions) {
            authorizer.revokePrivilegeFromRole(roleName, nodeName.getFullPath(), i);
          }
          break;
        case REVOKE_ROLE_FROM_USER:
          authorizer.revokeRoleFromUser(roleName, userName);
          break;
        default:
          throw new ProcessorException("Unsupported operation " + authorType);
      }
    } catch (AuthException e) {
      throw new ProcessorException(e);
    }
    return true;
  }

  private boolean operateWatermarkEmbedding(List<String> users, boolean useWatermark)
      throws ProcessorException {
    IAuthorizer authorizer;
    try {
      authorizer = LocalFileAuthorizer.getInstance();
      for (String user : users) {
        authorizer.setUserUseWaterMark(user, useWatermark);
      }
    } catch (AuthException e) {
      throw new ProcessorException(e);
    }
    return true;
  }
  
  private boolean createTimeSeries(CreateTimeSeriesPlan createTimeSeriesPlan) throws ProcessorException {
    Path path = createTimeSeriesPlan.getPath();
    TSDataType dataType = createTimeSeriesPlan.getDataType();
    CompressionType compressor = createTimeSeriesPlan.getCompressor();
    TSEncoding encoding = createTimeSeriesPlan.getEncoding();
    Map<String, String> props = createTimeSeriesPlan.getProps();
    try {
      boolean result = mManager.addPathToMTree(path, dataType, encoding, compressor, props);
      if (result) {
        storageEngine.addTimeSeries(path, dataType, encoding, compressor, props);
      }
    } catch (StorageEngineException | MetadataErrorException | PathErrorException e) {
      throw new ProcessorException(e);
    }
    return true;
  }
  
  private boolean deleteTimeSeries(DeleteTimeSeriesPlan deleteTimeSeriesPlan) throws ProcessorException {
    List<Path> deletePathList = deleteTimeSeriesPlan.getPaths();
    try {
      deleteDataOfTimeSeries(deletePathList);
      Set<String> emptyStorageGroups = mManager.deletePaths(deletePathList);
      for (String deleteStorageGroup : emptyStorageGroups) {
        storageEngine.deleteAllDataFilesInOneStorageGroup(deleteStorageGroup);
      }
    } catch (MetadataErrorException e) {
      throw new ProcessorException(e);
    }
    return true;
  }
  
  private boolean setStorageGroup(SetStorageGroupPlan setStorageGroupPlan) throws ProcessorException {
    Path path = setStorageGroupPlan.getPath();
    try {
      mManager.setStorageGroupToMTree(path.getFullPath());
    } catch (MetadataErrorException e) {
      throw new ProcessorException(e);
    }
    return true;
  }
  
  private boolean deleteStorageGroup(DeleteStorageGroupPlan deleteStorageGroupPlan) throws ProcessorException {
    List<Path> deletePathList = deleteStorageGroupPlan.getPaths();
    try {
      mManager.deleteStorageGroupsFromMTree(deletePathList);
      for (Path storageGroupPath : deletePathList) {
        storageEngine.deleteStorageGroup(storageGroupPath.getFullPath());
      }
    } catch (MetadataErrorException e) {
      throw new ProcessorException(e);
    }
    return true;
  }

  /**
   * Delete all data of time series in pathList.
   *
   * @param pathList deleted paths
   */
  private void deleteDataOfTimeSeries(List<Path> pathList) throws ProcessorException {
    for (Path p : pathList) {
      DeletePlan deletePlan = new DeletePlan();
      deletePlan.addPath(p);
      deletePlan.setDeleteTime(Long.MAX_VALUE);
      processNonQuery(deletePlan);
    }
  }

  private boolean operateProperty(PropertyPlan propertyPlan) throws ProcessorException {
    PropertyOperator.PropertyType propertyType = propertyPlan.getPropertyType();
    Path propertyPath = propertyPlan.getPropertyPath();
    Path metadataPath = propertyPlan.getMetadataPath();
    try {
      switch (propertyType) {
        case ADD_TREE:
          mManager.addAPTree(propertyPath.getFullPath());
          break;
        case ADD_PROPERTY_LABEL:
          mManager.addPathToPTree(propertyPath.getFullPath());
          break;
        case DELETE_PROPERTY_LABEL:
          mManager.deletePathFromPTree(propertyPath.getFullPath());
          break;
        case ADD_PROPERTY_TO_METADATA:
          mManager.linkMNodeToPTree(propertyPath.getFullPath(), metadataPath.getFullPath());
          break;
        case DEL_PROPERTY_FROM_METADATA:
          mManager.unlinkMNodeFromPTree(propertyPath.getFullPath(), metadataPath.getFullPath());
          break;
        default:
          throw new ProcessorException("unknown namespace type:" + propertyType);
      }
    } catch (PathErrorException | IOException | MetadataErrorException e) {
      throw new ProcessorException("meet error in " + propertyType + " . ", e);
    }
    return true;
  }

  @Override
  protected QueryDataSet processAuthorQuery(AuthorPlan plan, QueryContext context)
      throws ProcessorException {
    AuthorType authorType = plan.getAuthorType();
    String userName = plan.getUserName();
    String roleName = plan.getRoleName();
    Path path = plan.getNodeName();
    IAuthorizer authorizer;
    try {
      authorizer = LocalFileAuthorizer.getInstance();
    } catch (AuthException e) {
      throw new ProcessorException(e);
    }

    ListDataSet dataSet;

    try {
      switch (authorType) {
        case LIST_ROLE:
          dataSet = executeListRole(authorizer);
          break;
        case LIST_USER:
          dataSet = executeListUser(authorizer);
          break;
        case LIST_ROLE_USERS:
          dataSet = executeListRoleUsers(authorizer, roleName);
          break;
        case LIST_USER_ROLES:
          dataSet = executeListUserRoles(authorizer, userName);
          break;
        case LIST_ROLE_PRIVILEGE:
          dataSet = executeListRolePrivileges(authorizer, roleName, path);
          break;
        case LIST_USER_PRIVILEGE:
          dataSet = executeListUserPrivileges(authorizer, userName, path);
          break;
        default:
          throw new ProcessorException("Unsupported operation " + authorType);
      }
    } catch (AuthException e) {
      throw new ProcessorException(e);
    }
    return dataSet;
  }

  private ListDataSet executeListRole(IAuthorizer authorizer) {
    int index = 0;
    List<Path> headerList = new ArrayList<>();
    List<TSDataType> typeList = new ArrayList<>();
    headerList.add(new Path(ROLE));
    typeList.add(TSDataType.TEXT);
    ListDataSet dataSet = new ListDataSet(headerList, typeList);
    List<String> roleList = authorizer.listAllRoles();
    for (String role : roleList) {
      RowRecord record = new RowRecord(index++);
      Field field = new Field(TSDataType.TEXT);
      field.setBinaryV(new Binary(role));
      record.addField(field);
      dataSet.putRecord(record);
    }
    return dataSet;
  }

  private ListDataSet executeListUser(IAuthorizer authorizer) {
    List<String> userList = authorizer.listAllUsers();
    List<Path> headerList = new ArrayList<>();
    List<TSDataType> typeList = new ArrayList<>();
    headerList.add(new Path(USER));
    typeList.add(TSDataType.TEXT);
    int index = 0;
    ListDataSet dataSet = new ListDataSet(headerList, typeList);
    for (String user : userList) {
      RowRecord record = new RowRecord(index++);
      Field field = new Field(TSDataType.TEXT);
      field.setBinaryV(new Binary(user));
      record.addField(field);
      dataSet.putRecord(record);
    }
    return dataSet;
  }

  private ListDataSet executeListRoleUsers(IAuthorizer authorizer, String roleName)
      throws AuthException {
    Role role = authorizer.getRole(roleName);
    if (role == null) {
      throw new AuthException("No such role : " + roleName);
    }
    List<Path> headerList = new ArrayList<>();
    List<TSDataType> typeList = new ArrayList<>();
    headerList.add(new Path(USER));
    typeList.add(TSDataType.TEXT);
    ListDataSet dataSet = new ListDataSet(headerList, typeList);
    List<String> userList = authorizer.listAllUsers();
    int index = 0;
    for (String userN : userList) {
      User userObj = authorizer.getUser(userN);
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

  private ListDataSet executeListUserRoles(IAuthorizer authorizer, String userName)
      throws AuthException {
    User user = authorizer.getUser(userName);
    if (user != null) {
      List<Path> headerList = new ArrayList<>();
      List<TSDataType> typeList = new ArrayList<>();
      headerList.add(new Path(ROLE));
      typeList.add(TSDataType.TEXT);
      ListDataSet dataSet = new ListDataSet(headerList, typeList);
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

  private ListDataSet executeListRolePrivileges(IAuthorizer authorizer, String roleName, Path path)
      throws AuthException {
    Role role = authorizer.getRole(roleName);
    if (role != null) {
      List<Path> headerList = new ArrayList<>();
      List<TSDataType> typeList = new ArrayList<>();
      headerList.add(new Path(PRIVILEGE));
      typeList.add(TSDataType.TEXT);
      ListDataSet dataSet = new ListDataSet(headerList, typeList);
      int index = 0;
      for (PathPrivilege pathPrivilege : role.getPrivilegeList()) {
        if (path == null || AuthUtils
            .pathBelongsTo(path.getFullPath(), pathPrivilege.getPath())) {
          RowRecord record = new RowRecord(index++);
          Field field = new Field(TSDataType.TEXT);
          field.setBinaryV(new Binary(pathPrivilege.toString()));
          record.addField(field);
          dataSet.putRecord(record);
        }
      }
      return dataSet;
    } else {
      throw new AuthException("No such role : " + roleName);
    }
  }

  private ListDataSet executeListUserPrivileges(IAuthorizer authorizer, String userName, Path path)
      throws AuthException {
    User user = authorizer.getUser(userName);
    if (user == null) {
      throw new AuthException("No such user : " + userName);
    }
    List<Path> headerList = new ArrayList<>();
    List<TSDataType> typeList = new ArrayList<>();
    headerList.add(new Path(ROLE));
    headerList.add(new Path(PRIVILEGE));
    typeList.add(TSDataType.TEXT);
    typeList.add(TSDataType.TEXT);
    ListDataSet dataSet = new ListDataSet(headerList, typeList);
    int index = 0;
    for (PathPrivilege pathPrivilege : user.getPrivilegeList()) {
      if (path == null || AuthUtils.pathBelongsTo(path.getFullPath(), pathPrivilege.getPath())) {
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
    for (String roleN : user.getRoleList()) {
      Role role = authorizer.getRole(roleN);
      if (role == null) {
        continue;
      }
      for (PathPrivilege pathPrivilege : role.getPrivilegeList()) {
        if (path == null || AuthUtils.pathBelongsTo(path.getFullPath(), pathPrivilege.getPath())) {
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
    return dataSet;
  }

  /**
   * Add a seriesPath to MTree
   */
  private void addPathToMTree(String deviceId, String measurementId, TSDataType dataType)
      throws PathErrorException, MetadataErrorException, StorageEngineException {
    String fullPath = deviceId + IoTDBConstant.PATH_SEPARATOR + measurementId;
    TSEncoding defaultEncoding = getDefaultEncoding(dataType);
    CompressionType defaultCompressor =
        CompressionType.valueOf(TSFileDescriptor.getInstance().getConfig().getCompressor());
    boolean result = mManager.addPathToMTree(
        fullPath, dataType, defaultEncoding, defaultCompressor, Collections.emptyMap());
    if (result) {
      storageEngine.addTimeSeries(
          new Path(fullPath), dataType, defaultEncoding, defaultCompressor, Collections.emptyMap());
    }
  }

  private void addPathToMTree(String deviceId, String measurementId, Object value)
      throws PathErrorException, MetadataErrorException, StorageEngineException {
    TSDataType predictedDataType = TypeInferenceUtils.getPredictedDataType(value);
    addPathToMTree(deviceId, measurementId, predictedDataType);
  }

  /**
   * Get default encoding by dataType
   */
  private TSEncoding getDefaultEncoding(TSDataType dataType) {
    IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();
    switch (dataType) {
      case BOOLEAN:
        return conf.getDefaultBooleanEncoding();
      case INT32:
        return conf.getDefaultInt32Encoding();
      case INT64:
        return conf.getDefaultInt64Encoding();
      case FLOAT:
        return conf.getDefaultFloatEncoding();
      case DOUBLE:
        return conf.getDefaultDoubleEncoding();
      case TEXT:
        return conf.getDefaultTextEncoding();
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType.toString()));
    }
  }
}
