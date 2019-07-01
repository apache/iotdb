/**
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.auth.entity.PathPrivilege;
import org.apache.iotdb.db.auth.entity.PrivilegeType;
import org.apache.iotdb.db.auth.entity.Role;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.engine.filenodeV2.FileNodeManagerV2;
import org.apache.iotdb.db.exception.ArgsErrorException;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MNode;
import org.apache.iotdb.db.monitor.MonitorConstants;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator.AuthorType;
import org.apache.iotdb.db.qp.logical.sys.MetadataOperator;
import org.apache.iotdb.db.qp.logical.sys.PropertyOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadDataPlan;
import org.apache.iotdb.db.qp.physical.sys.MetadataPlan;
import org.apache.iotdb.db.qp.physical.sys.PropertyPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.AuthDataSet;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.db.utils.AuthUtils;
import org.apache.iotdb.db.utils.LoadDataUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OverflowQPExecutor extends QueryProcessExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(OverflowQPExecutor.class);

  private FileNodeManagerV2 fileNodeManager;
  private MManager mManager = MManager.getInstance();

  public OverflowQPExecutor() {
    fileNodeManager = FileNodeManagerV2.getInstance();
  }

  public static String checkValue(TSDataType dataType, String value) throws ProcessorException {
    if (dataType == TSDataType.BOOLEAN) {
      value = value.toLowerCase();
      if (SQLConstant.BOOLEAN_FALSE_NUM.equals(value)) {
        value = "false";
      } else if (SQLConstant.BOOLEAN_TRUE_NUM.equals(value)) {
        value = "true";
      } else if (!SQLConstant.BOOLEN_TRUE.equals(value) && !SQLConstant.BOOLEN_FALSE
          .equals(value)) {
        throw new ProcessorException("The BOOLEAN data type should be true/TRUE or false/FALSE");
      }
    } else if (dataType == TSDataType.TEXT) {
      if ((value.startsWith(SQLConstant.QUOTE) && value.endsWith(SQLConstant.QUOTE))
          || (value.startsWith(SQLConstant.DQUOTE) && value.endsWith(SQLConstant.DQUOTE))) {
        value = value.substring(1, value.length() - 1);
      } else {
        throw new ProcessorException("The TEXT data type should be covered by \" or '");
      }
    }
    return value;
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
        return insert((InsertPlan)plan);
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
      case LIST_ROLE:
      case LIST_USER:
      case LIST_ROLE_PRIVILEGE:
      case LIST_ROLE_USERS:
      case LIST_USER_PRIVILEGE:
      case LIST_USER_ROLES:
        throw new ProcessorException(String.format("Author query %s is now allowed"
            + " in processNonQuery", plan.getOperatorType()));
      case LOADDATA:
        LoadDataPlan loadData = (LoadDataPlan) plan;
        LoadDataUtils load = new LoadDataUtils();
        load.loadLocalDataMultiPass(loadData.getInputFilePath(), loadData.getMeasureType(),
            MManager.getInstance());
        return true;
      case DELETE_TIMESERIES:
      case CREATE_TIMESERIES:
      case SET_STORAGE_GROUP:
      case METADATA:
        MetadataPlan metadata = (MetadataPlan) plan;
        return operateMetadata(metadata);
      case PROPERTY:
        PropertyPlan property = (PropertyPlan) plan;
        return operateProperty(property);
      default:
        throw new UnsupportedOperationException(
            String.format("operation %s does not support", plan.getOperatorType()));
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
      throws ProcessorException, FileNodeManagerException, QueryFilterOptimizationException,
      PathErrorException, IOException {
    return queryRouter.aggregate(paths, aggres, expression, context);
  }

  @Override
  public QueryDataSet fill(List<Path> fillPaths, long queryTime, Map<TSDataType, IFill> fillTypes,
      QueryContext context)
      throws ProcessorException, IOException, PathErrorException, FileNodeManagerException {
    return queryRouter.fill(fillPaths, queryTime, fillTypes, context);
  }

  @Override
  public QueryDataSet groupBy(List<Path> paths, List<String> aggres, IExpression expression,
      long unit, long origin, List<Pair<Long, Long>> intervals, QueryContext context)
      throws ProcessorException, FileNodeManagerException, QueryFilterOptimizationException,
      PathErrorException, IOException {
    return queryRouter.groupBy(paths, aggres, expression, unit, origin, intervals, context);
  }

  @Override
  public boolean update(Path path, long startTime, long endTime, String value)
      throws ProcessorException {
    String deviceId = path.getDevice();
    String measurementId = path.getMeasurement();
    try {
      String fullPath = deviceId + "." + measurementId;
      if (!mManager.pathExist(fullPath)) {
        throw new ProcessorException(String.format("Time series %s does not exist.", fullPath));
      }
      mManager.getFileNameByPath(fullPath);
      TSDataType dataType = mManager.getSeriesType(fullPath);
      value = checkValue(dataType, value);
      fileNodeManager.update(deviceId, measurementId, startTime, endTime, dataType, value);
      return true;
    } catch (PathErrorException e) {
      throw new ProcessorException(e);
    }
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
      mManager.getFileNameByPath(path.getFullPath());
      fileNodeManager.delete(deviceId, measurementId, timestamp);
      return true;
    } catch (PathErrorException | FileNodeManagerException e) {
      throw new ProcessorException(e);
    }
  }


  @Override
  public boolean insert(InsertPlan insertPlan)
      throws ProcessorException {

    try {
      String[] measurementList = insertPlan.getMeasurements();
      String deviceId = insertPlan.getDeviceId();
      MNode node = mManager.getNodeByDeviceIdFromCache(insertPlan.getDeviceId());
      String[] values = insertPlan.getValues();
      TSDataType[] dataTypes = new TSDataType[measurementList.length];

      for (int i = 0; i < measurementList.length; i++) {
        if (!node.hasChild(measurementList[i])) {
          throw new ProcessorException(
              String.format("Current deviceId[%s] does not contains measurement:%s",
                  deviceId, measurementList[i]));
        }
        MNode measurementNode = node.getChild(measurementList[i]);
        if (!measurementNode.isLeaf()) {
          throw new ProcessorException(
              String.format("Current Path is not leaf node. %s.%s", deviceId,
                  measurementList[i]));
        }

        dataTypes[i] = measurementNode.getSchema().getType();
        values[i] = checkValue(dataTypes[i], values[i]);
      }
      insertPlan.setDataTypes(dataTypes);
      return fileNodeManager.insert(insertPlan);

    } catch (PathErrorException | FileNodeManagerException e) {
      throw new ProcessorException(e.getMessage());
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
          if (!authorizer.updateUserPassword(userName, newPassword)) {
            throw new ProcessorException("password " + newPassword + " is illegal");
          }
          return true;
        case CREATE_USER:
          if (!authorizer.createUser(userName, password)) {
            throw new ProcessorException("User " + userName + " already exists");
          }
          return true;
        case CREATE_ROLE:
          if (!authorizer.createRole(roleName)) {
            throw new ProcessorException("Role " + roleName + " already exists");
          }
          return true;
        case DROP_USER:
          if (!authorizer.deleteUser(userName)) {
            throw new ProcessorException("User " + userName + " does not exist");
          }
          return true;
        case DROP_ROLE:
          if (!authorizer.deleteRole(roleName)) {
            throw new ProcessorException("Role " + roleName + " does not exist");
          }
          return true;
        case GRANT_ROLE:
          for (int i : permissions) {
            if (!authorizer.grantPrivilegeToRole(roleName, nodeName.getFullPath(), i)) {
              throw new ProcessorException(
                  "Role " + roleName + " already has " + PrivilegeType.values()[i]
                      + " on " + nodeName.getFullPath());
            }
          }
          return true;
        case GRANT_USER:
          for (int i : permissions) {
            if (!authorizer.grantPrivilegeToUser(userName, nodeName.getFullPath(), i)) {
              throw new ProcessorException(
                  "User " + userName + " already has " + PrivilegeType.values()[i]
                      + " on " + nodeName.getFullPath());
            }
          }
          return true;
        case GRANT_ROLE_TO_USER:
          if (!authorizer.grantRoleToUser(roleName, userName)) {
            throw new ProcessorException("User " + userName + " already has role " + roleName);
          }
          return true;
        case REVOKE_USER:
          for (int i : permissions) {
            if (!authorizer.revokePrivilegeFromUser(userName, nodeName.getFullPath(), i)) {
              throw new ProcessorException(
                  "User " + userName + " does not have " + PrivilegeType.values()[i] + " on "
                      + nodeName);
            }
          }
          return true;
        case REVOKE_ROLE:
          for (int i : permissions) {
            if (!authorizer.revokePrivilegeFromRole(roleName, nodeName.getFullPath(), i)) {
              throw new ProcessorException(
                  "Role " + roleName + " does not have " + PrivilegeType.values()[i] + " on "
                      + nodeName);
            }
          }
          return true;
        case REVOKE_ROLE_FROM_USER:
          if (!authorizer.revokeRoleFromUser(roleName, userName)) {
            throw new ProcessorException("User " + userName + " does not have role " + roleName);
          }
          return true;
        default:
          throw new ProcessorException("Unsupported operation " + authorType);
      }
    } catch (AuthException e) {
      throw new ProcessorException(e.getMessage());
    }
  }

  private boolean operateMetadata(MetadataPlan metadataPlan) throws ProcessorException {
    MetadataOperator.NamespaceType namespaceType = metadataPlan.getNamespaceType();
    Path path = metadataPlan.getPath();
    TSDataType dataType = metadataPlan.getDataType();
    CompressionType compressor = metadataPlan.getCompressor();
    TSEncoding encoding = metadataPlan.getEncoding();
    Map<String, String> props = metadataPlan.getProps();
    List<Path> deletePathList = metadataPlan.getDeletePathList();
    try {
      switch (namespaceType) {
        case ADD_PATH:
          boolean isNewMeasurement = mManager.addPathToMTree(path, dataType, encoding, compressor
              , props);
          if (isNewMeasurement) {
            fileNodeManager.addTimeSeries(path, dataType, encoding, compressor, props);
          }
          break;
        case DELETE_PATH:
          deleteDataOfTimeSeries(deletePathList);
          Pair<Set<String>, Set<String>> closeDeletedStorageGroupPair =
              mManager.deletePathsFromMTree(deletePathList);
          for (String deleteStorageGroup : closeDeletedStorageGroupPair.right) {
            fileNodeManager.deleteOneFileNode(deleteStorageGroup);
          }
          break;
        case SET_FILE_LEVEL:
          mManager.setStorageLevelToMTree(path.getFullPath());
          break;
        default:
          throw new ProcessorException("unknown namespace type:" + namespaceType);
      }
    } catch (FileNodeManagerException | MetadataErrorException e) {
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
    MManager mManager = MManager.getInstance();
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
      throw new ProcessorException("meet error in " + propertyType + " . " + e.getMessage());
    }
    return true;
  }

  @Override
  protected QueryDataSet processAuthorQuery(AuthorPlan plan, QueryContext context)
      throws ProcessorException {
    AuthorType authorType = plan.getAuthorType();
    String userName = plan.getUserName();
    String roleName = plan.getRoleName();
    Path nodeName = plan.getNodeName();
    IAuthorizer authorizer;
    try {
      authorizer = LocalFileAuthorizer.getInstance();
    } catch (AuthException e) {
      throw new ProcessorException(e);
    }
    List<Path> headerList = new ArrayList<>();
    List<TSDataType> typeList = new ArrayList<>();
    List<String> roleList;
    List<String> userList;
    AuthDataSet dataSet;
    int index = 0;
    try {
      switch (authorType) {
        case LIST_ROLE:
          headerList.add(new Path(ROLE));
          typeList.add(TSDataType.TEXT);
          dataSet = new AuthDataSet(headerList, typeList);
          roleList = authorizer.listAllRoles();
          for (String role : roleList) {
            RowRecord record = new RowRecord(index++);
            Field field = new Field(TSDataType.TEXT);
            field.setBinaryV(new Binary(role));
            record.addField(field);
            dataSet.putRecord(record);
          }
          break;
        case LIST_USER:
          userList = authorizer.listAllUsers();
          headerList.add(new Path(USER));
          typeList.add(TSDataType.TEXT);
          dataSet = new AuthDataSet(headerList, typeList);
          for (String user : userList) {
            RowRecord record = new RowRecord(index++);
            Field field = new Field(TSDataType.TEXT);
            field.setBinaryV(new Binary(user));
            record.addField(field);
            dataSet.putRecord(record);
          }
          break;
        case LIST_ROLE_USERS:
          Role role = authorizer.getRole(roleName);
          if (role == null) {
            throw new ProcessorException("No such role : " + roleName);
          }
          headerList.add(new Path(USER));
          typeList.add(TSDataType.TEXT);
          dataSet = new AuthDataSet(headerList, typeList);
          userList = authorizer.listAllUsers();
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
          break;
        case LIST_USER_ROLES:
          User user = authorizer.getUser(userName);
          if (user != null) {
            headerList.add(new Path(ROLE));
            typeList.add(TSDataType.TEXT);
            dataSet = new AuthDataSet(headerList, typeList);
            for (String roleN : user.getRoleList()) {
              RowRecord record = new RowRecord(index++);
              Field field = new Field(TSDataType.TEXT);
              field.setBinaryV(new Binary(roleN));
              record.addField(field);
              dataSet.putRecord(record);
            }
          } else {
            throw new ProcessorException("No such user : " + userName);
          }
          break;
        case LIST_ROLE_PRIVILEGE:
          role = authorizer.getRole(roleName);
          if (role != null) {
            headerList.add(new Path(PRIVILEGE));
            typeList.add(TSDataType.TEXT);
            dataSet = new AuthDataSet(headerList, typeList);
            for (PathPrivilege pathPrivilege : role.getPrivilegeList()) {
              if (nodeName == null || AuthUtils
                  .pathBelongsTo(nodeName.getFullPath(), pathPrivilege.getPath())) {
                RowRecord record = new RowRecord(index++);
                Field field = new Field(TSDataType.TEXT);
                field.setBinaryV(new Binary(pathPrivilege.toString()));
                record.addField(field);
                dataSet.putRecord(record);
              }
            }
          } else {
            throw new ProcessorException("No such role : " + roleName);
          }
          break;
        case LIST_USER_PRIVILEGE:
          user = authorizer.getUser(userName);
          if (user == null) {
            throw new ProcessorException("No such user : " + userName);
          }
          headerList.add(new Path(ROLE));
          headerList.add(new Path(PRIVILEGE));
          typeList.add(TSDataType.TEXT);
          typeList.add(TSDataType.TEXT);
          dataSet = new AuthDataSet(headerList, typeList);
          for (PathPrivilege pathPrivilege : user.getPrivilegeList()) {
            if (nodeName == null || AuthUtils
                .pathBelongsTo(nodeName.getFullPath(), pathPrivilege.getPath())) {
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
            role = authorizer.getRole(roleN);
            if (role != null) {
              for (PathPrivilege pathPrivilege : role.getPrivilegeList()) {
                if (nodeName == null
                    || AuthUtils.pathBelongsTo(nodeName.getFullPath(), pathPrivilege.getPath())) {
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
          break;
        default:
          throw new ProcessorException("Unsupported operation " + authorType);
      }
    } catch (AuthException e) {
      throw new ProcessorException(e.getMessage());
    }
    return dataSet;
  }
}
