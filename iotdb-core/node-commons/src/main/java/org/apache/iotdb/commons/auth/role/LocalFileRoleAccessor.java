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
package org.apache.iotdb.commons.auth.role;

import org.apache.iotdb.commons.auth.entity.DatabasePrivilege;
import org.apache.iotdb.commons.auth.entity.IEntryAccessor;
import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.IOUtils;

import org.apache.thrift.TException;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * This class store role info in a separate sequential file and every role will have one role file.
 * Role file schema: rolename-length int32| rolename-bytes-utf8| system-perivileges int32|
 * path[1]-length int32| path[1]-bytes-utf8 | privilege-int32| path[2]-length int32|
 * path[2]-bytes-utf8 | privilege-int32| path[3]-length int32| path[3]-bytes-utf8 | privilege-int32|
 * .....
 *
 * <p>system-privileges is an int32 mask code. Low 16 bits for privileges and high 16 bits mark that
 * whether the corresponding position 's privilege has grant option.So we can use a int32 to mark 16
 * kinds of privileges. Here are the permissions corresponding to the bit location identifier：
 * Manage_database : 0 MANAGE_USER : 1 MANAGE_ROLE : 2 USE_TRIGGER : 3 USE_UDF : 4 USE_CQ : 5
 * USE_PIPE : 6 EXTEDN_TEMPLATE : 7 AUDIT : 8 MAINTAIN : 9 |0000-0000-0000-0001|0000-0000-0000-0001|
 * means this role has Manage_database's privilege and be able to grant it to others.
 *
 * <p>path-privileges is a pair contains paths and privileges mask. Path privilege is a int32 mask
 * code which has the same structure as system-privileges. For path privilege, the low 16 bits stand
 * for four privileges, they are: READ_DATA : 0 WRITE_DATA : 1 READ_SCHEMA : 2 WRITE_SCHEMA : 3 And
 * the high 16 bits stand for grant option.
 *
 * <p>Role files is appendable.
 */

/**
 * All user/role file store in config node's user/role folder. when our system start, we load all
 * user/role from folder. If we did some alter query, we just store raft log.
 */
public class LocalFileRoleAccessor implements IEntryAccessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileRoleAccessor.class);
  protected static final String TEMP_SUFFIX = ".temp";
  protected static final String STRING_ENCODING = "utf-8";
  protected final String entryDirPath;
  protected static final int VERSION = 1;

  /**
   * Reused buffer for primitive types encoding/decoding, which aim to reduce memory fragments. Use
   * ThreadLocal for thread safety.
   */
  protected final ThreadLocal<ByteBuffer> encodingBufferLocal = new ThreadLocal<>();

  protected final ThreadLocal<byte[]> strBufferLocal = new ThreadLocal<>();

  public LocalFileRoleAccessor(String roleDirPath) {
    this.entryDirPath = roleDirPath;
  }

  protected String getEntrySnapshotFileName() {
    return "system" + File.separator + "roles";
  }

  protected void saveEntryVersion(BufferedOutputStream outputStream) throws IOException {
    IOUtils.writeInt(outputStream, VERSION, encodingBufferLocal);
  }

  protected void saveEntryName(BufferedOutputStream outputStream, Role role) throws IOException {
    byte[] strBuffer = role.getName().getBytes(STRING_ENCODING);
    IOUtils.writeInt(outputStream, strBuffer.length, encodingBufferLocal);
    outputStream.write(strBuffer);
  }

  protected void savePrivileges(BufferedOutputStream outputStream, Role role) throws IOException {
    IOUtils.writeInt(outputStream, role.getAllSysPrivileges(), encodingBufferLocal);
    int privilegeNum = role.getPathPrivilegeList().size();
    IOUtils.writeInt(outputStream, privilegeNum, encodingBufferLocal);
    for (int i = 0; i < privilegeNum; i++) {
      PathPrivilege pathPrivilege = role.getPathPrivilegeList().get(i);
      IOUtils.writePathPrivilege(outputStream, pathPrivilege, STRING_ENCODING, encodingBufferLocal);
    }
    privilegeNum = role.getObjectPrivilegeMap().size();
    IOUtils.writeInt(outputStream, privilegeNum, encodingBufferLocal);
    for (Map.Entry<String, DatabasePrivilege> objectPrivilegeMap :
        role.getObjectPrivilegeMap().entrySet()) {
      IOUtils.writeObjectPrivilege(
          outputStream, objectPrivilegeMap.getValue(), STRING_ENCODING, encodingBufferLocal);
    }
  }

  protected void loadPrivileges(DataInputStream dataInputStream, Role role)
      throws IOException, IllegalPathException {
    int num = ReadWriteIOUtils.readInt(dataInputStream);
    List<PathPrivilege> pathPrivilegeList = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      pathPrivilegeList.add(
          IOUtils.readPathPrivilege(dataInputStream, STRING_ENCODING, strBufferLocal));
    }
    role.setPrivilegeList(pathPrivilegeList);
    Map<String, DatabasePrivilege> objectPrivilegeMap = new HashMap<>();
    num = ReadWriteIOUtils.readInt(dataInputStream);
    for (int i = 0; i < num; i++) {
      DatabasePrivilege databasePrivilege =
          IOUtils.readObjectPrivilege(dataInputStream, STRING_ENCODING, strBufferLocal);
      objectPrivilegeMap.put(databasePrivilege.getDatabaseName(), databasePrivilege);
    }
    role.setObjectPrivilegeMap(objectPrivilegeMap);
  }

  protected void saveRoles(Role role) throws IOException {
    // Do nothing.
  }

  protected File checkFileAvaliable(String entryName, String suffix) {
    File userProfile =
        SystemFileFactory.INSTANCE.getFile(
            entryDirPath + File.separator + entryName + suffix + IoTDBConstant.PROFILE_SUFFIX);
    if (!userProfile.exists() || !userProfile.isFile()) {
      // System may crush before a newer file is renamed.
      File newProfile =
          SystemFileFactory.INSTANCE.getFile(
              entryDirPath
                  + File.separator
                  + entryName
                  + suffix
                  + IoTDBConstant.PROFILE_SUFFIX
                  + TEMP_SUFFIX);
      if (newProfile.exists() && newProfile.isFile()) {
        if (!newProfile.renameTo(userProfile)) {
          LOGGER.error("New profile renaming not succeed.");
        }
        userProfile = newProfile;
      } else {
        return null;
      }
    }
    return userProfile;
  }

  @Override
  public Role loadEntry(String entryName) throws IOException {
    File entryFile = checkFileAvaliable(entryName, "");
    if (entryFile == null) {
      return null;
    }

    FileInputStream inputStream = new FileInputStream(entryFile);
    try (DataInputStream dataInputStream =
        new DataInputStream(new BufferedInputStream(inputStream))) {
      int version = dataInputStream.readInt();
      entryName = IOUtils.readString(dataInputStream, STRING_ENCODING, strBufferLocal);
      Role role = new Role(entryName);
      role.setSysPrivilegesWithMask(dataInputStream.readInt());
      loadPrivileges(dataInputStream, role);
      return role;
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      strBufferLocal.remove();
    }
  }

  @Override
  public void saveEntry(Role role) throws IOException {
    File roleProfile =
        SystemFileFactory.INSTANCE.getFile(
            entryDirPath
                + File.separator
                + role.getName()
                + IoTDBConstant.PROFILE_SUFFIX
                + TEMP_SUFFIX);
    File roleDir = new File(entryDirPath);
    if (!roleDir.exists() && !roleDir.mkdirs()) {
      LOGGER.error("Failed to create role dir {}", entryDirPath);
    }

    try (FileOutputStream fileOutputStream = new FileOutputStream(roleProfile);
        BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream)) {
      saveEntryVersion(outputStream);
      saveEntryName(outputStream, role);
      savePrivileges(outputStream, role);
      outputStream.flush();
      fileOutputStream.getFD().sync();
    } catch (Exception e) {
      LOGGER.warn("meet error when save role: {}", role.getName());
      throw new IOException(e);
    } finally {
      encodingBufferLocal.remove();
    }

    File oldFile =
        SystemFileFactory.INSTANCE.getFile(
            entryDirPath + File.separator + role.getName() + IoTDBConstant.PROFILE_SUFFIX);
    IOUtils.replaceFile(roleProfile, oldFile);
    saveRoles(role);
  }

  @Override
  public boolean deleteEntry(String entryName) throws IOException {
    File roleProfile =
        SystemFileFactory.INSTANCE.getFile(
            entryDirPath + File.separator + entryName + IoTDBConstant.PROFILE_SUFFIX);
    File backFile =
        SystemFileFactory.INSTANCE.getFile(
            entryDirPath + File.separator + entryName + IoTDBConstant.PROFILE_SUFFIX + TEMP_SUFFIX);
    if (!roleProfile.exists() && !backFile.exists()) {
      return false;
    }
    if ((roleProfile.exists() && !roleProfile.delete())
        || (backFile.exists() && !backFile.delete())) {
      throw new IOException(String.format("Cannot delete role file of %s", entryName));
    }
    return true;
  }

  @Override
  public List<String> listAllEntries() {
    File roleDir = SystemFileFactory.INSTANCE.getFile(entryDirPath);
    String[] names =
        roleDir.list(
            (dir, name) ->
                name.endsWith(IoTDBConstant.PROFILE_SUFFIX) || name.endsWith(TEMP_SUFFIX));
    List<String> retList = new ArrayList<>();
    if (names != null) {
      // in very rare situations, normal file and backup file may exist at the same time
      // so a set is used to deduplicate
      Set<String> set = new HashSet<>();
      for (String fileName : names) {
        set.add(fileName.replace(IoTDBConstant.PROFILE_SUFFIX, "").replace(TEMP_SUFFIX, ""));
      }
      retList.addAll(set);
    }
    return retList;
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    SystemFileFactory systemFileFactory = SystemFileFactory.INSTANCE;
    File roleFolder = systemFileFactory.getFile(entryDirPath);
    File roleSnapshotDir = systemFileFactory.getFile(snapshotDir, getEntrySnapshotFileName());
    File roleTmpSnapshotDir =
        systemFileFactory.getFile(roleSnapshotDir.getAbsolutePath() + "-" + UUID.randomUUID());

    boolean result = true;
    try {
      result = FileUtils.copyDir(roleFolder, roleTmpSnapshotDir);
      result &= roleTmpSnapshotDir.renameTo(roleSnapshotDir);
    } finally {
      if (roleTmpSnapshotDir.exists() && !roleTmpSnapshotDir.delete()) {
        FileUtils.deleteFileOrDirectory(roleTmpSnapshotDir);
      }
    }
    return result;
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    SystemFileFactory systemFileFactory = SystemFileFactory.INSTANCE;
    File roleFolder = systemFileFactory.getFile(entryDirPath);
    File roleTmpFolder =
        systemFileFactory.getFile(roleFolder.getAbsolutePath() + "-" + UUID.randomUUID());
    File roleSnapshotDir = systemFileFactory.getFile(snapshotDir, getEntrySnapshotFileName());
    if (roleSnapshotDir.exists()) {
      try {
        org.apache.commons.io.FileUtils.moveDirectory(roleFolder, roleTmpFolder);
        if (!FileUtils.copyDir(roleSnapshotDir, roleFolder)) {
          LOGGER.error("Failed to load role folder snapshot and rollback.");
          // rollback if failed to copy
          FileUtils.deleteFileOrDirectory(roleFolder);
          org.apache.commons.io.FileUtils.moveDirectory(roleTmpFolder, roleFolder);
        }
      } finally {
        FileUtils.deleteFileOrDirectory(roleTmpFolder);
      }
    } else {
      LOGGER.info("There are no roles to load.");
    }
  }

  @Override
  public void reset() {
    checkOldEntryDir(SystemFileFactory.INSTANCE.getFile(entryDirPath));
    if (SystemFileFactory.INSTANCE.getFile(entryDirPath).mkdirs()) {
      LOGGER.info("role info dir {} is created", entryDirPath);
    } else if (!SystemFileFactory.INSTANCE.getFile(entryDirPath).exists()) {
      LOGGER.error("role info dir {} can not be created", entryDirPath);
    }
  }

  private void checkOldEntryDir(File newDir) {
    File oldDir = new File(CommonDescriptor.getInstance().getConfig().getOldRoleFolder());
    if (oldDir.exists()) {
      if (!FileUtils.moveFileSafe(oldDir, newDir)) {
        LOGGER.error("move old role dir fail: {}", oldDir.getAbsolutePath());
      }
    }
  }

  @Override
  public void cleanEntryFolder() {
    File[] files = SystemFileFactory.INSTANCE.getFile(entryDirPath).listFiles();
    if (files != null) {
      for (File file : files) {
        FileUtils.deleteFileIfExist(file);
      }
    } else {
      LOGGER.warn("Role folder not exists");
    }
  }
}
