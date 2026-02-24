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

import org.apache.iotdb.commons.auth.entity.PrivilegeModelType;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorRelationalPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorTreePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeCreateTableOrViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.persistence.schema.mnode.IConfigMNode;
import org.apache.iotdb.confignode.persistence.schema.mnode.factory.ConfigMNodeFactory;
import org.apache.iotdb.confignode.persistence.schema.mnode.impl.ConfigTableNode;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;

import org.apache.tsfile.external.commons.io.IOUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.commons.schema.SchemaConstant.DATABASE_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.INTERNAL_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.TABLE_MNODE_TYPE;
import static org.apache.iotdb.commons.utils.IOUtils.readString;

public class CNPhysicalPlanGenerator
    implements Iterator<ConfigPhysicalPlan>, Iterable<ConfigPhysicalPlan> {

  private final Logger logger = LoggerFactory.getLogger(CNPhysicalPlanGenerator.class);
  private final IMNodeFactory<IConfigMNode> nodeFactory = ConfigMNodeFactory.getInstance();

  // File input stream.
  private InputStream inputStream = null;
  private InputStream templateInputStream = null;

  private static final String STRING_ENCODING = "utf-8";

  private final ThreadLocal<byte[]> strBufferLocal = new ThreadLocal<>();

  private final HashMap<Integer, String> templateTable = new HashMap<>();

  private final List<IConfigMNode> templateNodeList = new ArrayList<>();

  // All plan will be stored at this deque
  private final Deque<ConfigPhysicalPlan> planDeque = new ArrayDeque<>();

  private CNSnapshotFileType snapshotFileType = CNSnapshotFileType.INVALID;

  private Exception latestException = null;
  private String userName;

  public CNPhysicalPlanGenerator(
      final Path snapshotFilePath, final CNSnapshotFileType fileType, final String userName)
      throws IOException {
    if (fileType == CNSnapshotFileType.SCHEMA) {
      logger.warn("schema_template need two files");
      return;
    }
    if (fileType == CNSnapshotFileType.USER_ROLE) {
      this.userName = userName;
    }
    snapshotFileType = fileType;
    inputStream = Files.newInputStream(snapshotFilePath);
  }

  public CNPhysicalPlanGenerator(final Path schemaInfoFile, final Path templateFile)
      throws IOException {
    inputStream = Files.newInputStream(schemaInfoFile);
    // Template file is null for table mTree
    if (Objects.nonNull(templateFile)) {
      templateInputStream = Files.newInputStream(templateFile);
    }
    snapshotFileType = CNSnapshotFileType.SCHEMA;
  }

  @Nonnull
  @Override
  @SuppressWarnings("java:S4348")
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
    } else if (snapshotFileType == CNSnapshotFileType.TTL) {
      generateSetTTLPlan();
    } else if (snapshotFileType == CNSnapshotFileType.SCHEMA) {
      if (Objects.nonNull(templateInputStream)) {
        generateTemplatePlan();
      }
      if (latestException != null) {
        return false;
      }
      generateDatabasePhysicalPlan();
      if (latestException != null) {
        return false;
      }
      generateSetTemplatePlan();
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
    } catch (final IOException ioException) {
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

  private void generateUserRolePhysicalPlan(final boolean isUser) {
    try (final DataInputStream dataInputStream =
        new DataInputStream(new BufferedInputStream(inputStream))) {
      int tag = dataInputStream.readInt();
      String user;
      if (tag < 0) {
        user = readString(dataInputStream, STRING_ENCODING, strBufferLocal, -1 * tag);
      } else if (tag == 1) {
        user = readString(dataInputStream, STRING_ENCODING, strBufferLocal);
      } else {
        if (isUser) {
          dataInputStream.readLong(); // skip userId since authorPlan do not demand it.
        }
        user = readString(dataInputStream, STRING_ENCODING, strBufferLocal);
      }

      if (isUser) {
        final String rawPassword = readString(dataInputStream, STRING_ENCODING, strBufferLocal);
        final AuthorTreePlan createUser =
            new AuthorTreePlan(ConfigPhysicalPlanType.CreateUserWithRawPassword);
        createUser.setUserName(user);
        createUser.setPassword(rawPassword);
        createUser.setPermissions(new HashSet<>());
        createUser.setNodeNameList(new ArrayList<>());
        planDeque.add(createUser);
        if (tag == 2) {
          final AuthorTreePlan updateUserMaxSession =
              new AuthorTreePlan(ConfigPhysicalPlanType.UpdateUserMaxSession);
          updateUserMaxSession.setMaxSessionPerUser(dataInputStream.readInt());
          updateUserMaxSession.setUserName(user);
          updateUserMaxSession.setPermissions(new HashSet<>());
          updateUserMaxSession.setNodeNameList(new ArrayList<>());
          planDeque.add(updateUserMaxSession);
          final AuthorTreePlan updateUserMinSession =
              new AuthorTreePlan(ConfigPhysicalPlanType.UpdateUserMinSession);
          updateUserMinSession.setMinSessionPerUser(dataInputStream.readInt());
          updateUserMinSession.setUserName(user);
          updateUserMinSession.setPermissions(new HashSet<>());
          updateUserMinSession.setNodeNameList(new ArrayList<>());
          planDeque.add(updateUserMinSession);
        }

      } else {
        final AuthorTreePlan createRole = new AuthorTreePlan(ConfigPhysicalPlanType.CreateRole);
        createRole.setRoleName(user);
        createRole.setPermissions(new HashSet<>());
        createRole.setNodeNameList(new ArrayList<>());
        planDeque.add(createRole);
      }

      final int privilegeMask = dataInputStream.readInt();
      generateGrantSysPlan(user, isUser, privilegeMask);

      if (tag < 0) {
        while (dataInputStream.available() != 0) {
          final String path = readString(dataInputStream, STRING_ENCODING, strBufferLocal);
          final PartialPath priPath;
          try {
            priPath = new PartialPath(path);
          } catch (IllegalPathException exception) {
            latestException = exception;
            return;
          }
          int privileges = dataInputStream.readInt();
          generateGrantAuthorTreePlan(user, isUser, priPath, privileges);
        }
      } else {
        int num = dataInputStream.readInt();
        for (int i = 0; i < num; i++) {
          final String path = readString(dataInputStream, STRING_ENCODING, strBufferLocal);
          final PartialPath priPath;
          try {
            priPath = new PartialPath(path);
          } catch (IllegalPathException exception) {
            latestException = exception;
            return;
          }
          int privileges = dataInputStream.readInt();
          generateGrantAuthorTreePlan(user, isUser, priPath, privileges);
        }
        int anyScopePriv = dataInputStream.readInt();
        generateGrantAuthorRelationalPlan(user, isUser, null, null, anyScopePriv);

        num = dataInputStream.readInt();
        for (int i = 0; i < num; i++) {
          final String databaseName = readString(dataInputStream, STRING_ENCODING, strBufferLocal);
          int databasePrivilege = dataInputStream.readInt();
          generateGrantAuthorRelationalPlan(user, isUser, databaseName, null, databasePrivilege);
          int tableNum = dataInputStream.readInt();
          for (int tableid = 0; tableid < tableNum; tableid++) {
            final String tableName = readString(dataInputStream, STRING_ENCODING, strBufferLocal);
            int tablePrivilege = dataInputStream.readInt();
            generateGrantAuthorRelationalPlan(
                user, isUser, databaseName, tableName, tablePrivilege);
          }
        }
      }
    } catch (IOException ioException) {
      logger.error(
          "Got IOException when deserialize use&role file, type:{}", snapshotFileType, ioException);
      latestException = ioException;
    } finally {
      strBufferLocal.remove();
    }
  }

  private void generateGrantRolePhysicalPlan() {
    try (final DataInputStream roleInputStream =
        new DataInputStream(new BufferedInputStream((inputStream)))) {
      for (int i = 0; roleInputStream.available() != 0; i++) {
        final String roleName = readString(roleInputStream, STRING_ENCODING, strBufferLocal);
        final AuthorTreePlan plan = new AuthorTreePlan(ConfigPhysicalPlanType.GrantRoleToUser);
        plan.setUserName(userName);
        plan.setRoleName(roleName);
        plan.setNodeNameList(new ArrayList<>());
        plan.setPermissions(new HashSet<>());
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
    for (int i = 0; i < PrivilegeType.getPrivilegeCount(PrivilegeModelType.SYSTEM); i++) {
      if ((sysMask & (1 << i)) != 0) {
        final AuthorTreePlan plan =
            new AuthorTreePlan(
                isUser ? ConfigPhysicalPlanType.GrantUser : ConfigPhysicalPlanType.GrantRole);
        if (isUser) {
          plan.setUserName(userName);
          plan.setRoleName("");
        } else {
          plan.setRoleName(userName);
          plan.setUserName("");
        }
        plan.setPermissions(Collections.singleton(AuthUtils.posToSysPri(i).ordinal()));
        if ((sysMask & (1 << (i + 16))) != 0) {
          plan.setGrantOpt(true);
        }
        plan.setNodeNameList(new ArrayList<>());
        planDeque.add(plan);
      }
    }
  }

  private void generateSetTTLPlan() {
    try (final DataInputStream ttlInputStream =
        new DataInputStream(new BufferedInputStream(inputStream))) {
      int size = ReadWriteIOUtils.readInt(ttlInputStream);
      while (size > 0) {
        final String path = ReadWriteIOUtils.readString(ttlInputStream);
        final long ttl = ReadWriteIOUtils.readLong(ttlInputStream);
        planDeque.add(new SetTTLPlan(PathUtils.splitPathToDetachedNodes(path), ttl));
        size--;
      }
    } catch (final IOException | IllegalPathException e) {
      logger.error("Got exception when deserializing ttl file", e);
      latestException = e;
    }
  }

  private void generateGrantAuthorTreePlan(
      String name, boolean isUser, PartialPath path, int priMask) {
    for (int pos = 0; pos < PrivilegeType.getPrivilegeCount(PrivilegeModelType.TREE); pos++) {
      if (((1 << pos) & priMask) != 0) {
        final AuthorTreePlan plan =
            new AuthorTreePlan(
                isUser ? ConfigPhysicalPlanType.GrantUser : ConfigPhysicalPlanType.GrantRole);
        if (isUser) {
          plan.setUserName(name);
          plan.setRoleName("");
        } else {
          plan.setRoleName(name);
          plan.setUserName("");
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

  private void generateGrantAuthorRelationalPlan(
      String name, boolean isUser, String database, String table, int priMask) {
    for (int pos = 0; pos < PrivilegeType.getPrivilegeCount(PrivilegeModelType.RELATIONAL); pos++) {
      if (((1 << pos) & priMask) != 0) {
        final AuthorRelationalPlan plan;
        if (database == null && table == null) {
          plan =
              new AuthorRelationalPlan(
                  isUser
                      ? ConfigPhysicalPlanType.RGrantUserAny
                      : ConfigPhysicalPlanType.RGrantRoleAny);
        } else if (database != null && table == null) {
          plan =
              new AuthorRelationalPlan(
                  isUser
                      ? ConfigPhysicalPlanType.RGrantUserDBPriv
                      : ConfigPhysicalPlanType.RGrantRoleDBPriv);
        } else {
          plan =
              new AuthorRelationalPlan(
                  isUser
                      ? ConfigPhysicalPlanType.RGrantUserTBPriv
                      : ConfigPhysicalPlanType.RGrantRoleTBPriv);
        }
        if (isUser) {
          plan.setUserName(name);
          plan.setRoleName("");
        } else {
          plan.setRoleName(name);
          plan.setUserName("");
        }
        plan.setPermissions(Collections.singleton(AuthUtils.posToObjPri(pos).ordinal()));
        if ((1 << (pos + 16) & priMask) != 0) {
          plan.setGrantOpt(true);
        }
        plan.setDatabaseName(database == null ? "" : database);
        plan.setTableName(table == null ? "" : table);
        planDeque.add(plan);
      }
    }
  }

  // NOTE: The stack is reserved for mTree creation, to unify the logic and to get full path
  // info for schema template set. Do not touch this
  private void generateDatabasePhysicalPlan() {
    try (final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream)) {
      byte type = ReadWriteIOUtils.readByte(bufferedInputStream);
      String name;
      int childNum;
      final Stack<Pair<IConfigMNode, Boolean>> stack = new Stack<>();
      IConfigMNode databaseMNode;
      IConfigMNode internalMNode;
      ConfigTableNode tableNode;

      final Set<TsTable> tableSet = new HashSet<>();

      if (type == DATABASE_MNODE_TYPE) {
        databaseMNode = deserializeDatabaseMNode(bufferedInputStream);
        name = databaseMNode.getName();
        stack.push(new Pair<>(databaseMNode, true));
      } else if (type == TABLE_MNODE_TYPE) {
        tableNode = ConfigMTree.deserializeTableMNode(bufferedInputStream);
        name = tableNode.getName();
        stack.push(new Pair<>(tableNode, false));
        tableSet.add(tableNode.getTable());
      } else {
        internalMNode = deserializeInternalMNode(bufferedInputStream);
        // Child num
        ReadWriteIOUtils.readInt(bufferedInputStream);
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
          case DATABASE_MNODE_TYPE:
            databaseMNode = deserializeDatabaseMNode(bufferedInputStream).getAsMNode();
            while (!stack.isEmpty() && !stack.peek().right) {
              databaseMNode.addChild(stack.pop().left);
            }
            stack.push(new Pair<>(databaseMNode, true));
            name = databaseMNode.getName();
            for (final TsTable table : tableSet) {
              planDeque.add(new PipeCreateTableOrViewPlan(name, table));
            }
            tableSet.clear();
            break;
          case TABLE_MNODE_TYPE:
            tableNode = ConfigMTree.deserializeTableMNode(bufferedInputStream);
            name = tableNode.getName();
            stack.push(new Pair<>(tableNode, false));
            tableSet.add(tableNode.getTable());
            break;
          default:
            logger.error("Unrecognized node type. Cannot deserialize MTree from given buffer");
            return;
        }
      }
    } catch (final IOException ioException) {
      logger.error("Got IOException when construct database Tree", ioException);
      latestException = ioException;
    }
  }

  private void generateTemplatePlan() {
    try (final BufferedInputStream bufferedInputStream =
        new BufferedInputStream(templateInputStream)) {
      final ByteBuffer byteBuffer = ByteBuffer.wrap(IOUtils.toByteArray(bufferedInputStream));
      // Skip id
      ReadWriteIOUtils.readInt(byteBuffer);
      int size = ReadWriteIOUtils.readInt(byteBuffer);
      while (size > 0) {
        final Template template = new Template();
        template.deserialize(byteBuffer);
        templateTable.put(template.getId(), template.getName());
        template.setId(0);
        final CreateSchemaTemplatePlan plan =
            new CreateSchemaTemplatePlan(template.serialize().array());
        planDeque.add(plan);
        size--;
      }
    } catch (IOException ioException) {
      logger.error("Got IOException when deserialize template info", ioException);
      latestException = ioException;
    }
  }

  private void generateSetTemplatePlan() {
    if (templateNodeList.isEmpty()) {
      return;
    }
    for (final IConfigMNode templateNode : templateNodeList) {
      final String templateName = templateTable.get(templateNode.getSchemaTemplateId());
      final CommitSetSchemaTemplatePlan plan =
          new CommitSetSchemaTemplatePlan(templateName, templateNode.getFullPath());
      planDeque.add(plan);
    }
  }

  private IConfigMNode deserializeDatabaseMNode(final InputStream inputStream) throws IOException {
    final IDatabaseMNode<IConfigMNode> databaseMNode =
        nodeFactory.createDatabaseMNode(null, ReadWriteIOUtils.readString(inputStream));
    databaseMNode.getAsMNode().setSchemaTemplateId(ReadWriteIOUtils.readInt(inputStream));
    databaseMNode
        .getAsMNode()
        .setDatabaseSchema(ThriftConfigNodeSerDeUtils.deserializeTDatabaseSchema(inputStream));

    if (databaseMNode.getAsMNode().getSchemaTemplateId() >= 0 && !templateTable.isEmpty()) {
      templateNodeList.add((IConfigMNode) databaseMNode);
    }

    final TDatabaseSchema schema = databaseMNode.getAsMNode().getDatabaseSchema();
    if (!schema.getName().equals(SchemaConstant.AUDIT_DATABASE)
        && !schema.getName().equals(SchemaConstant.SYSTEM_DATABASE)) {
      final DatabaseSchemaPlan createDBPlan =
          new DatabaseSchemaPlan(ConfigPhysicalPlanType.CreateDatabase, schema);
      planDeque.add(createDBPlan);
    }
    return databaseMNode.getAsMNode();
  }

  private IConfigMNode deserializeInternalMNode(final InputStream inputStream) throws IOException {
    final IConfigMNode basicMNode =
        nodeFactory.createInternalMNode(null, ReadWriteIOUtils.readString(inputStream));
    basicMNode.setSchemaTemplateId(ReadWriteIOUtils.readInt(inputStream));
    if (basicMNode.getSchemaTemplateId() >= 0 && !templateTable.isEmpty()) {
      templateNodeList.add(basicMNode);
    }
    return basicMNode;
  }
}
