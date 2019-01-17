/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.qp.executor;

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
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.exception.ArgsErrorException;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.metadata.ColumnSchema;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MNode;
import org.apache.iotdb.db.monitor.MonitorConstants;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
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
import org.apache.iotdb.db.utils.AuthUtils;
import org.apache.iotdb.db.utils.LoadDataUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OverflowQPExecutor extends QueryProcessExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(OverflowQPExecutor.class);

  private FileNodeManager fileNodeManager;
  private MManager mManager = MManager.getInstance();

  public OverflowQPExecutor() {
    fileNodeManager = FileNodeManager.getInstance();
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
        DeletePlan delete = (DeletePlan) plan;
        return delete(delete.getPaths(), delete.getDeleteTime());
      case UPDATE:
        UpdatePlan update = (UpdatePlan) plan;
        boolean flag = true;
        for (Pair<Long, Long> timePair : update.getIntervals()) {
          flag &= update(update.getPath(), timePair.left, timePair.right, update.getValue());
        }
        return flag;
      case INSERT:
        InsertPlan insert = (InsertPlan) plan;
        int result = multiInsert(insert.getDeviceId(), insert.getTime(), insert.getMeasurements(),
            insert.getValues());
        return result > 0;
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
      case LIST_ROLE:
      case LIST_USER:
      case LIST_ROLE_PRIVILEGE:
      case LIST_ROLE_USERS:
      case LIST_USER_PRIVILEGE:
      case LIST_USER_ROLES:
        AuthorPlan author = (AuthorPlan) plan;
        return operateAuthor(author);
      case LOADDATA:
        LoadDataPlan loadData = (LoadDataPlan) plan;
        LoadDataUtils load = new LoadDataUtils();
        load.loadLocalDataMultiPass(loadData.getInputFilePath(), loadData.getMeasureType(),
            MManager.getInstance());
        return true;
      case DELETE_TIMESERIES:
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
  public QueryDataSet aggregate(List<Pair<Path, String>> aggres, IExpression expression)
      throws ProcessorException {
    throw new ProcessorException("not support");
  }

  // @Override
  // public QueryDataSet fill(List<Path> fillPaths, long queryTime, Map<TSDataType, IFill> fillTypes) throws
  // ProcessorException, IOException, PathErrorException {
  // return queryEngine.fill(fillPaths, queryTime, fillTypes);
  // }

  @Override
  public QueryDataSet groupBy(List<Pair<Path, String>> aggres, IExpression expression, long unit,
      long origin,
      List<Pair<Long, Long>> intervals, int fetchSize) throws ProcessorException {
    throw new ProcessorException("not support");
  }

  @Override
  public boolean update(Path path, long startTime, long endTime, String value)
      throws ProcessorException {
    String deviceId = path.getDevice();
    String measurementId = path.getMeasurement();
    try {
      String fullPath = deviceId + "." + measurementId;
      if (!mManager.pathExist(fullPath)) {
        throw new ProcessorException(String.format("Timeseries %s does not exist.", fullPath));
      }
      mManager.getFileNameByPath(fullPath);
      TSDataType dataType = mManager.getSeriesType(fullPath);
      value = checkValue(dataType, value);
      fileNodeManager.update(deviceId, measurementId, startTime, endTime, dataType, value);
      return true;
    } catch (PathErrorException e) {
      throw new ProcessorException(e.getMessage());
    } catch (FileNodeManagerException e) {
      e.printStackTrace();
      throw new ProcessorException(e.getMessage());
    }
  }

  @Override
  protected boolean delete(Path path, long timestamp) throws ProcessorException {
    String deviceId = path.getDevice();
    String measurementId = path.getMeasurement();
    try {
      if (!mManager.pathExist(path.getFullPath())) {
        throw new ProcessorException(
            String.format("Timeseries %s does not exist.", path.getFullPath()));
      }
      mManager.getFileNameByPath(path.getFullPath());
      TSDataType type = mManager.getSeriesType(path.getFullPath());
      fileNodeManager.delete(deviceId, measurementId, timestamp, type);
      return true;
    } catch (PathErrorException e) {
      throw new ProcessorException(e.getMessage());
    } catch (FileNodeManagerException e) {
      e.printStackTrace();
      throw new ProcessorException(e.getMessage());
    }
  }

  @Override
  // return 0: failed, 1: Overflow, 2:Bufferwrite
  public int insert(Path path, long timestamp, String value) throws ProcessorException {
    String deviceId = path.getDevice();
    String measurementId = path.getMeasurement();

    try {
      TSDataType type = mManager.getSeriesType(deviceId + "," + measurementId);
      TSRecord tsRecord = new TSRecord(timestamp, deviceId);
      DataPoint dataPoint = DataPoint.getDataPoint(type, measurementId, value);
      tsRecord.addTuple(dataPoint);
      return fileNodeManager.insert(tsRecord, false);

    } catch (PathErrorException e) {
      throw new ProcessorException("Error in insert: " + e.getMessage());
    } catch (FileNodeManagerException e) {
      e.printStackTrace();
      throw new ProcessorException(e);
    }
  }

  @Override
  public int multiInsert(String deviceId, long insertTime, List<String> measurementList,
      List<String> insertValues)
      throws ProcessorException {
    try {
      TSRecord tsRecord = new TSRecord(insertTime, deviceId);

      MNode node = mManager.getNodeByDeviceIdFromCache(deviceId);

      for (int i = 0; i < measurementList.size(); i++) {
        if (!node.hasChild(measurementList.get(i))) {
          throw new ProcessorException(
              String.format("Current deviceId[%s] does not contains measurement:%s",
                  deviceId, measurementList.get(i)));
        }
        MNode measurementNode = node.getChild(measurementList.get(i));
        if (!measurementNode.isLeaf()) {
          throw new ProcessorException(
              String.format("Current Path is not leaf node. %s.%s", deviceId,
                  measurementList.get(i)));
        }

        TSDataType dataType = measurementNode.getSchema().dataType;
        String value = insertValues.get(i);
        value = checkValue(dataType, value);
        DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementList.get(i), value);
        tsRecord.addTuple(dataPoint);
      }
      return fileNodeManager.insert(tsRecord, false);

    } catch (PathErrorException | FileNodeManagerException e) {
      throw new ProcessorException(e.getMessage());
    }
  }

  @Override
  public List<String> getAllPaths(String originPath) throws PathErrorException {
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
    IAuthorizer authorizer = null;
    try {
      authorizer = LocalFileAuthorizer.getInstance();
    } catch (AuthException e) {
      throw new ProcessorException(e);
    }
    StringBuilder msg;
    List<String> roleList;
    List<String> userList;
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
        case LIST_ROLE:
          roleList = authorizer.listAllRoles();
          msg = new StringBuilder("Roles are : [ \n");
          for (String role : roleList) {
            msg.append(role).append("\n");
          }
          msg.append("]");
          // TODO : use a more elegant way to pass message.
          throw new ProcessorException(msg.toString());
        case LIST_USER:
          userList = authorizer.listAllUsers();
          msg = new StringBuilder("Users are : [ \n");
          for (String user : userList) {
            msg.append(user).append("\n");
          }
          msg.append("]");
          throw new ProcessorException(msg.toString());
        case LIST_ROLE_USERS:
          Role role = authorizer.getRole(roleName);
          if (role == null) {
            throw new ProcessorException("No such role : " + roleName);
          }
          userList = authorizer.listAllUsers();
          msg = new StringBuilder("Users are : [ \n");
          for (String userN : userList) {
            User userObj = authorizer.getUser(userN);
            if (userObj != null && userObj.hasRole(roleName)) {
              msg.append(userN).append("\n");
            }
          }
          msg.append("]");
          throw new ProcessorException(msg.toString());
        case LIST_USER_ROLES:
          msg = new StringBuilder("Roles are : [ \n");
          User user = authorizer.getUser(userName);
          if (user != null) {
            for (String roleN : user.roleList) {
              msg.append(roleN).append("\n");
            }
          } else {
            throw new ProcessorException("No such user : " + userName);
          }
          msg.append("]");
          throw new ProcessorException(msg.toString());
        case LIST_ROLE_PRIVILEGE:
          msg = new StringBuilder("Privileges are : [ \n");
          role = authorizer.getRole(roleName);
          if (role != null) {
            for (PathPrivilege pathPrivilege : role.privilegeList) {
              if (nodeName == null || AuthUtils
                  .pathBelongsTo(nodeName.getFullPath(), pathPrivilege.path)) {
                msg.append(pathPrivilege.toString());
              }
            }
          } else {
            throw new ProcessorException("No such role : " + roleName);
          }
          msg.append("]");
          throw new ProcessorException(msg.toString());
        case LIST_USER_PRIVILEGE:
          user = authorizer.getUser(userName);
          if (user == null) {
            throw new ProcessorException("No such user : " + userName);
          }
          msg = new StringBuilder("Privileges are : [ \n");
          msg.append("From itself : {\n");
          for (PathPrivilege pathPrivilege : user.privilegeList) {
            if (nodeName == null || AuthUtils
                .pathBelongsTo(nodeName.getFullPath(), pathPrivilege.path)) {
              msg.append(pathPrivilege.toString());
            }
          }
          msg.append("}\n");
          for (String roleN : user.roleList) {
            role = authorizer.getRole(roleN);
            if (role != null) {
              msg.append("From role ").append(roleN).append(" : {\n");
              for (PathPrivilege pathPrivilege : role.privilegeList) {
                if (nodeName == null
                    || AuthUtils.pathBelongsTo(nodeName.getFullPath(), pathPrivilege.path)) {
                  msg.append(pathPrivilege.toString());
                }
              }
              msg.append("}\n");
            }
          }
          msg.append("]");
          throw new ProcessorException(msg.toString());
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
    String dataType = metadataPlan.getDataType();
    String encoding = metadataPlan.getEncoding();
    String[] encodingArgs = metadataPlan.getEncodingArgs();
    List<Path> deletePathList = metadataPlan.getDeletePathList();
    try {
      switch (namespaceType) {
        case ADD_PATH:
          if (mManager.pathExist(path.getFullPath())) {
            throw new ProcessorException(
                String.format("Timeseries %s already exist", path.getFullPath()));
          }
          if (!mManager.checkFileNameByPath(path.getFullPath())) {
            throw new ProcessorException("Storage group should be created first");
          }
          // optimize the speed of adding timeseries
          String fileNodePath = mManager.getFileNameByPath(path.getFullPath());
          // the two map is stored in the storage group node
          Map<String, ColumnSchema> schemaMap = mManager.getSchemaMapForOneFileNode(fileNodePath);
          Map<String, Integer> numSchemaMap = mManager.getNumSchemaMapForOneFileNode(fileNodePath);
          String lastNode = path.getMeasurement();
          boolean isNewMeasurement = true;
          // Thread safety: just one thread can access/modify the schemaMap
          synchronized (schemaMap) {
            if (schemaMap.containsKey(lastNode)) {
              isNewMeasurement = false;
              ColumnSchema columnSchema = schemaMap.get(lastNode);
              if (!columnSchema.geTsDataType().toString().equals(dataType)
                  || !columnSchema.getEncoding().toString().equals(encoding)) {
                throw new ProcessorException(String.format(
                    "The dataType or encoding of the last node %s is conflicting in the storage group %s",
                    lastNode, fileNodePath));
              }
              mManager.addPathToMTree(path.getFullPath(), dataType, encoding, encodingArgs);
              numSchemaMap.put(lastNode, numSchemaMap.get(lastNode) + 1);
            } else {
              mManager.addPathToMTree(path.getFullPath(), dataType, encoding, encodingArgs);
              ColumnSchema columnSchema = mManager.getSchemaForOnePath(path.toString());
              schemaMap.put(lastNode, columnSchema);
              numSchemaMap.put(lastNode, 1);
            }
            try {
              if (isNewMeasurement) {
                // add time series to schema
                fileNodeManager.addTimeSeries(path, dataType, encoding, encodingArgs);
              }
              // fileNodeManager.closeOneFileNode(namespacePath);
            } catch (FileNodeManagerException e) {
              throw new ProcessorException(e);
            }
          }
          break;
        case DELETE_PATH:
          if (deletePathList != null && !deletePathList.isEmpty()) {
            Set<String> pathSet = new HashSet<>();
            // Attention: Monitor storage group seriesPath is not allowed to be deleted
            for (Path p : deletePathList) {
              ArrayList<String> subPaths = mManager.getPaths(p.getFullPath());
              if (subPaths.isEmpty()) {
                throw new ProcessorException(String
                    .format("There are no timeseries in the prefix of %s seriesPath",
                        p.getFullPath()));
              }
              ArrayList<String> newSubPaths = new ArrayList<>();
              for (String eachSubPath : subPaths) {
                String filenodeName = mManager.getFileNameByPath(eachSubPath);

                if (MonitorConstants.statStorageGroupPrefix.equals(filenodeName)) {
                  continue;
                }
                newSubPaths.add(eachSubPath);
              }
              pathSet.addAll(newSubPaths);
            }
            for (String p : pathSet) {
              if (!mManager.pathExist(p)) {
                throw new ProcessorException(String.format(
                    "Timeseries %s does not exist and cannot be delete its metadata and data", p));
              }
            }
            List<String> fullPath = new ArrayList<>();
            fullPath.addAll(pathSet);
            try {
              deleteDataOfTimeSeries(fullPath);
            } catch (ProcessorException e) {
              throw new ProcessorException(e);
            }
            Set<String> closeFileNodes = new HashSet<>();
            Set<String> deleteFielNodes = new HashSet<>();
            for (String p : fullPath) {
              String nameSpacePath = null;
              try {
                nameSpacePath = mManager.getFileNameByPath(p);
              } catch (PathErrorException e) {
                throw new ProcessorException(e);
              }
              closeFileNodes.add(nameSpacePath);
              // the two map is stored in the storage group node
              schemaMap = mManager.getSchemaMapForOneFileNode(nameSpacePath);
              numSchemaMap = mManager.getNumSchemaMapForOneFileNode(nameSpacePath);
              // Thread safety: just one thread can access/modify the schemaMap
              synchronized (schemaMap) {
                // TODO: don't delete the storage group seriesPath
                // recursively
                path = new Path(p);
                String measurementId = path.getMeasurement();
                if (numSchemaMap.get(measurementId) == 1) {
                  numSchemaMap.remove(measurementId);
                  schemaMap.remove(measurementId);
                } else {
                  numSchemaMap.put(measurementId, numSchemaMap.get(measurementId) - 1);
                }
                String deleteNameSpacePath = mManager.deletePathFromMTree(p);
                if (deleteNameSpacePath != null) {
                  deleteFielNodes.add(deleteNameSpacePath);
                }
              }
            }
            closeFileNodes.removeAll(deleteFielNodes);
            for (String deleteFileNode : deleteFielNodes) {
              // close processor
              fileNodeManager.deleteOneFileNode(deleteFileNode);
            }
            for (String closeFileNode : closeFileNodes) {
              fileNodeManager.closeOneFileNode(closeFileNode);
            }
          }
          break;
        case SET_FILE_LEVEL:
          mManager.setStorageLevelToMTree(path.getFullPath());
          break;
        default:
          throw new ProcessorException("unknown namespace type:" + namespaceType);
      }
    } catch (PathErrorException | IOException | ArgsErrorException | FileNodeManagerException e) {
      throw new ProcessorException(e.getMessage());
    }
    return true;
  }

  /**
   * Delete all data of time series in pathList.
   *
   * @param pathList
   *            deleted paths
   */
  private void deleteDataOfTimeSeries(List<String> pathList) throws ProcessorException {
    for (String p : pathList) {
      DeletePlan deletePlan = new DeletePlan();
      deletePlan.addPath(new Path(p));
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
    } catch (PathErrorException | IOException | ArgsErrorException e) {
      throw new ProcessorException("meet error in " + propertyType + " . " + e.getMessage());
    }
    return true;
  }
}
