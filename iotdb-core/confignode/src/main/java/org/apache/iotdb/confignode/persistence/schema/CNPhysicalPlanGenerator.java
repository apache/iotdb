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

package org.apache.iotdb.confignode.persistence.schema;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.persistence.schema.mnode.IConfigMNode;
import org.apache.iotdb.confignode.persistence.schema.mnode.factory.ConfigMNodeFactory;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Stack;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.commons.schema.SchemaConstant.INTERNAL_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.STORAGE_GROUP_MNODE_TYPE;
import static org.apache.iotdb.commons.utils.IOUtils.readAuthString;
import static org.apache.iotdb.commons.utils.IOUtils.readString;

public class CNPhysicalPlanGenerator
    implements Iterator<ConfigPhysicalPlan>, Iterable<ConfigPhysicalPlan> {

  private final Logger logger = LoggerFactory.getLogger(CNPhysicalPlanGenerator.class);
  private final IMNodeFactory<IConfigMNode> nodeFactory = ConfigMNodeFactory.getInstance();

  // File input stream.
  private InputStream inputStream = null;
  private InputStream templateInputStream = null;

  private static final String STRING_ENCODING = "utf-8";

  // For default password
  private static final String DEFAULT_PASSWORD = "password";
  private final ThreadLocal<byte[]> strBufferLocal = new ThreadLocal<>();

  private final HashMap<Integer, String> templateTable = new HashMap<>();

  // All plan will be stored at this deque
  private final Deque<ConfigPhysicalPlan> planDeque = new ArrayDeque<>();

  private CNSnapshotFileType snapshotFileType = CNSnapshotFileType.INVALID;

  private Exception latestException = null;
  private String userName;

  public CNPhysicalPlanGenerator(Path snapshotFilePath, CNSnapshotFileType fileType)
      throws IOException {
    if (fileType == CNSnapshotFileType.SCHEMA_TEMPLATE) {
      logger.warn("schema_template need two files");
      return;
    }
    if (fileType == CNSnapshotFileType.USER_ROLE) {
      userName = snapshotFilePath.getFileName().toString().split("_role.profile")[0];
    }
    snapshotFileType = fileType;
    inputStream = Files.newInputStream(snapshotFilePath);
  }

  public CNPhysicalPlanGenerator(Path schemaInfoFile, Path templateFile) throws IOException {
    inputStream = Files.newInputStream(schemaInfoFile);
    templateInputStream = Files.newInputStream(templateFile);
    snapshotFileType = CNSnapshotFileType.SCHEMA_TEMPLATE;
  }

  @Override
  public Iterator<ConfigPhysicalPlan> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    if (!planDeque.isEmpty()) {
      return true;
    }

    if (snapshotFileType == CNSnapshotFileType.USER) {
      generateUserRolePhysicalPlan(true);
    } else if (snapshotFileType == CNSnapshotFileType.ROLE) {
      generateUserRolePhysicalPlan(false);
    } else if (snapshotFileType == CNSnapshotFileType.USER_ROLE) {
      generateGrantRolePhysicalPlan();
    } else if (snapshotFileType == CNSnapshotFileType.SCHEMA_TEMPLATE) {
      generateTemplatePlan();
      if (latestException != null) {
        return false;
      }
      generateDatabasePhysicalPlan();
    }
    snapshotFileType = CNSnapshotFileType.INVALID;
    try {
      if (inputStream != null) {
        inputStream.close();
        inputStream = null;
      }
      if (templateInputStream != null) {
        templateInputStream.close();
        templateInputStream = null;
      }
    } catch (IOException ioException) {
      latestException = ioException;
    }

    if (latestException != null) {
      return false;
    }
    return !planDeque.isEmpty();
  }

  @Override
  public ConfigPhysicalPlan next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return planDeque.pop();
  }

  public void checkException() throws Exception {
    if (latestException != null) {
      throw new Exception(latestException.getMessage());
    }
  }

  private void generateUserRolePhysicalPlan(boolean isUser) {
    try (DataInputStream dataInputStream =
        new DataInputStream(new BufferedInputStream(inputStream))) {
      Pair<String, Boolean> versionAndName =
          readAuthString(dataInputStream, STRING_ENCODING, strBufferLocal);
      if (versionAndName == null) {
        return;
      }
      String user = versionAndName.left;
      if (isUser) {
        // skip password
        readString(dataInputStream, STRING_ENCODING, strBufferLocal);
        AuthorPlan createUser = new AuthorPlan(ConfigPhysicalPlanType.CreateUser);
        createUser.setUserName(user);
        createUser.setPassword(DEFAULT_PASSWORD);
        planDeque.add(createUser);
      } else {
        AuthorPlan createRole = new AuthorPlan(ConfigPhysicalPlanType.CreateRole);
        createRole.setRoleName(user);
        planDeque.add(createRole);
      }

      int privilegeMask = dataInputStream.readInt();
      // translate sys privileges
      generateGrantSysPlan(user, isUser, privilegeMask);
      // translate path privileges
      while (dataInputStream.available() != 0) {
        String path = readString(dataInputStream, STRING_ENCODING, strBufferLocal);
        PartialPath priPath;
        try {
          priPath = new PartialPath(path);
        } catch (IllegalPathException exception) {
          latestException = exception;
          return;
        }
        int privileges = dataInputStream.readInt();
        generateGrantPathPlan(user, isUser, priPath, privileges);
      }
    } catch (IOException ioException) {
      logger.error(
          "Got IOException when deserialize userole file, type:{}", snapshotFileType, ioException);
      latestException = ioException;
    } finally {
      strBufferLocal.remove();
    }
  }

  private void generateGrantRolePhysicalPlan() {
    try (DataInputStream roleInputStream =
        new DataInputStream(new BufferedInputStream((inputStream)))) {
      while (roleInputStream.available() != 0) {
        String roleName = readString(roleInputStream, STRING_ENCODING, strBufferLocal);
        AuthorPlan plan = new AuthorPlan(ConfigPhysicalPlanType.GrantRoleToUser);
        plan.setUserName(userName);
        plan.setRoleName(roleName);
        planDeque.add(plan);
      }
    } catch (IOException ioException) {
      logger.error("Got IOException when deserialize roleList", ioException);
      latestException = ioException;
    } finally {
      strBufferLocal.remove();
    }
  }

  private void generateGrantSysPlan(String userName, boolean isUser, int sysMask) {
    for (int i = 0; i < PrivilegeType.getSysPriCount(); i++) {
      if ((sysMask & (1 << i)) != 0) {
        AuthorPlan plan =
            new AuthorPlan(
                isUser ? ConfigPhysicalPlanType.GrantUser : ConfigPhysicalPlanType.GrantRole);
        if (isUser) {
          plan.setUserName(userName);
        } else {
          plan.setRoleName(userName);
        }
        plan.setPermissions(Collections.singleton(AuthUtils.posToSysPri(i)));
        if ((sysMask & (1 << (i + 16))) != 0) {
          plan.setGrantOpt(true);
        }
        plan.setNodeNameList(new ArrayList<>());
        planDeque.add(plan);
      }
    }
  }

  private void generateGrantPathPlan(
      String userName, boolean isUser, PartialPath path, int priMask) {
    for (int pos = 0; pos < PrivilegeType.getPathPriCount(); pos++) {
      if (((1 << pos) & priMask) != 0) {
        AuthorPlan plan =
            new AuthorPlan(
                isUser ? ConfigPhysicalPlanType.GrantUser : ConfigPhysicalPlanType.GrantRole);
        if (isUser) {
          plan.setUserName(userName);
        } else {
          plan.setRoleName(userName);
        }
        plan.setPermissions(Collections.singleton(AuthUtils.pathPosToPri(pos)));
        plan.setNodeNameList(Collections.singletonList(path));
        if ((1 << (pos + 16) & priMask) != 0) {
          plan.setGrantOpt(true);
        }
        planDeque.add(plan);
      }
    }
  }

  private void generateDatabasePhysicalPlan() {
    try (BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream)) {
      byte type = ReadWriteIOUtils.readByte(bufferedInputStream);
      String name = null;
      int childNum = 0;
      Stack<Pair<IConfigMNode, Boolean>> stack = new Stack<>();
      IConfigMNode databaseMNode;
      IConfigMNode internalMNode;

      if (type == STORAGE_GROUP_MNODE_TYPE) {
        databaseMNode = deserializeDatabaseMNode(bufferedInputStream);
        name = databaseMNode.getName();
        stack.push(new Pair<>(databaseMNode, true));
      } else {
        internalMNode = deserializeInternalMNode(bufferedInputStream);
        childNum = ReadWriteIOUtils.readInt(bufferedInputStream);
        name = internalMNode.getName();
        stack.push(new Pair<>(internalMNode, false));
      }

      while (!PATH_ROOT.equals(name)) {
        type = ReadWriteIOUtils.readByte(bufferedInputStream);
        switch (type) {
          case INTERNAL_MNODE_TYPE:
            internalMNode = deserializeInternalMNode(bufferedInputStream);
            childNum = ReadWriteIOUtils.readInt(bufferedInputStream);
            boolean hasDB = false;
            while (childNum > 0) {
              hasDB = stack.peek().right;
              internalMNode.addChild(stack.pop().left);
              childNum--;
            }
            stack.push(new Pair<>(internalMNode, hasDB));
            name = internalMNode.getName();
            break;
          case STORAGE_GROUP_MNODE_TYPE:
            databaseMNode = deserializeDatabaseMNode(bufferedInputStream).getAsMNode();
            while (!stack.isEmpty() && !stack.peek().right) {
              databaseMNode.addChild(stack.pop().left);
            }
            stack.push(new Pair<>(databaseMNode, true));
            name = databaseMNode.getName();
            break;
          default:
            logger.error("Unrecognized node type. Cannot deserialize MTree from given buffer");
            return;
        }
      }
    } catch (IOException ioException) {
      logger.error("Got IOException when construct database Tree", ioException);
      latestException = ioException;
    }
  }

  private void generateTemplatePlan() {
    try (BufferedInputStream bufferedInputStream = new BufferedInputStream(templateInputStream)) {
      ByteBuffer byteBuffer = ByteBuffer.wrap(IOUtils.toByteArray(bufferedInputStream));
      // skip id
      ReadWriteIOUtils.readInt(byteBuffer);
      int size = ReadWriteIOUtils.readInt(byteBuffer);
      while (size > 0) {
        Template template = new Template();
        template.deserialize(byteBuffer);
        template.setId(0);
        templateTable.put(template.getId(), template.getName());
        CreateSchemaTemplatePlan plan = new CreateSchemaTemplatePlan(template.serialize().array());
        planDeque.add(plan);
        size--;
      }
    } catch (IOException ioException) {
      logger.error("Got IOException when deserialize template info", ioException);
      latestException = ioException;
    }
  }

  private IConfigMNode deserializeDatabaseMNode(InputStream inputStream) throws IOException {
    IDatabaseMNode<IConfigMNode> databaseMNode =
        nodeFactory.createDatabaseMNode(null, ReadWriteIOUtils.readString(inputStream));
    databaseMNode.getAsMNode().setSchemaTemplateId(ReadWriteIOUtils.readInt(inputStream));
    databaseMNode
        .getAsMNode()
        .setDatabaseSchema(ThriftConfigNodeSerDeUtils.deserializeTDatabaseSchema(inputStream));
    if (databaseMNode.getAsMNode().getDatabaseSchema().isSetTTL()) {
      SetTTLPlan plan =
          new SetTTLPlan(
              Collections.singletonList(databaseMNode.getAsMNode().getDatabaseSchema().getName()),
              databaseMNode.getAsMNode().getDatabaseSchema().getTTL());
      planDeque.add(plan);
      databaseMNode.getAsMNode().getDatabaseSchema().unsetTTL();
    }

    DatabaseSchemaPlan createDBPlan =
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, databaseMNode.getAsMNode().getDatabaseSchema());
    planDeque.add(createDBPlan);

    return databaseMNode.getAsMNode();
  }

  private IConfigMNode deserializeInternalMNode(InputStream inputStream) throws IOException {
    IConfigMNode basicMNode =
        nodeFactory.createInternalMNode(null, ReadWriteIOUtils.readString(inputStream));
    basicMNode.setSchemaTemplateId(ReadWriteIOUtils.readInt(inputStream));
    if (basicMNode.getSchemaTemplateId() >= 0) {
      if (!templateTable.isEmpty()) {
        String templateName = templateTable.get(basicMNode.getSchemaTemplateId());
        // ignore preset plan.
        CommitSetSchemaTemplatePlan plan =
            new CommitSetSchemaTemplatePlan(templateName, basicMNode.getFullPath());
        planDeque.add(plan);
      }
    }
    return basicMNode;
  }
}
