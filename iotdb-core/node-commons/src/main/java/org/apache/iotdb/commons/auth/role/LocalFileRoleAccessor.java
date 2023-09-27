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

import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.IOUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
public class LocalFileRoleAccessor implements IRoleAccessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileRoleAccessor.class);
  private static final String TEMP_SUFFIX = ".temp";
  private static final String STRING_ENCODING = "utf-8";
  private static final String ROLE_SNAPSHOT_FILE_NAME = "system" + File.separator + "roles";

  private final String roleDirPath;

  /**
   * Reused buffer for primitive types encoding/decoding, which aim to reduce memory fragments. Use
   * ThreadLocal for thread safety.
   */
  private final ThreadLocal<ByteBuffer> encodingBufferLocal = new ThreadLocal<>();

  private final ThreadLocal<byte[]> strBufferLocal = new ThreadLocal<>();

  public LocalFileRoleAccessor(String roleDirPath) {
    this.roleDirPath = roleDirPath;
  }

  /**
   * @return role struct
   * @throws IOException
   */
  @Override
  public Role loadRole(String rolename) throws IOException {
    File roleProfile =
        SystemFileFactory.INSTANCE.getFile(
            roleDirPath + File.separator + rolename + IoTDBConstant.PROFILE_SUFFIX);
    if (!roleProfile.exists() || !roleProfile.isFile()) {
      // System may crush before a newer file is written, so search for back-up file.
      File backProfile =
          SystemFileFactory.INSTANCE.getFile(
              roleDirPath + File.separator + rolename + IoTDBConstant.PROFILE_SUFFIX + TEMP_SUFFIX);
      if (backProfile.exists() && backProfile.isFile()) {
        roleProfile = backProfile;
      } else {
        return null;
      }
    }
    FileInputStream inputStream = new FileInputStream(roleProfile);
    try (DataInputStream dataInputStream =
        new DataInputStream(new BufferedInputStream(inputStream))) {
      Role role = new Role();
      Pair<String, Boolean> result =
          IOUtils.readAuthString(dataInputStream, STRING_ENCODING, strBufferLocal);
      role.setName(result.getLeft());
      boolean oldVersion = result.getRight();
      if (oldVersion) {
        IOUtils.loadRolePrivilege(role, dataInputStream, STRING_ENCODING, strBufferLocal);
        return role;
      } else {
        role.setSysPrivilegeSet(dataInputStream.readInt());
        List<PathPrivilege> pathPrivilegeList = new ArrayList<>();
        for (int i = 0; dataInputStream.available() != 0; i++) {
          pathPrivilegeList.add(
              IOUtils.readPathPrivilege(dataInputStream, STRING_ENCODING, strBufferLocal, false));
        }
        role.setPrivilegeList(pathPrivilegeList);
        return role;
      }
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      strBufferLocal.remove();
    }
  }

  @Override
  public void saveRole(Role role) throws IOException {
    File roleProfile =
        SystemFileFactory.INSTANCE.getFile(
            roleDirPath
                + File.separator
                + role.getName()
                + IoTDBConstant.PROFILE_SUFFIX
                + TEMP_SUFFIX);
    File roleDir = new File(roleDirPath);
    if (!roleDir.exists()) {
      if (!roleDir.mkdirs()) {
        LOGGER.error("Failed to create role dir {}", roleDirPath);
      }
    }
    try (BufferedOutputStream outputStream =
        new BufferedOutputStream(Files.newOutputStream(roleProfile.toPath()))) {
      try {
        byte[] strBuffer = role.getName().getBytes(STRING_ENCODING);
        IOUtils.writeInt(outputStream, -1 * strBuffer.length, encodingBufferLocal);
        outputStream.write(strBuffer);
        IOUtils.writeInt(outputStream, role.getAllSysPrivileges(), encodingBufferLocal);
        int privilegeNum = role.getPathPrivilegeList().size();
        for (int i = 0; i < privilegeNum; i++) {
          PathPrivilege pathPrivilege = role.getPathPrivilegeList().get(i);
          IOUtils.writePathPrivilege(
              outputStream, pathPrivilege, STRING_ENCODING, encodingBufferLocal);
        }
        outputStream.flush();
      } catch (Exception e) {
        throw new IOException(e);
      }
    } finally {
      encodingBufferLocal.remove();
    }

    File oldFile =
        SystemFileFactory.INSTANCE.getFile(
            roleDirPath + File.separator + role.getName() + IoTDBConstant.PROFILE_SUFFIX);
    IOUtils.replaceFile(roleProfile, oldFile);
  }

  @Override
  public boolean deleteRole(String rolename) throws IOException {
    File roleProfile =
        SystemFileFactory.INSTANCE.getFile(
            roleDirPath + File.separator + rolename + IoTDBConstant.PROFILE_SUFFIX);
    File backFile =
        SystemFileFactory.INSTANCE.getFile(
            roleDirPath + File.separator + rolename + IoTDBConstant.PROFILE_SUFFIX + TEMP_SUFFIX);
    if (!roleProfile.exists() && !backFile.exists()) {
      return false;
    }
    if ((roleProfile.exists() && !roleProfile.delete())
        || (backFile.exists() && !backFile.delete())) {
      throw new IOException(String.format("Cannot delete role file of %s", rolename));
    }
    return true;
  }

  @Override
  public List<String> listAllRoles() {
    File roleDir = SystemFileFactory.INSTANCE.getFile(roleDirPath);
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
    File roleFolder = systemFileFactory.getFile(roleDirPath);
    File roleSnapshotDir = systemFileFactory.getFile(snapshotDir, ROLE_SNAPSHOT_FILE_NAME);
    File roleTmpSnapshotDir =
        systemFileFactory.getFile(roleSnapshotDir.getAbsolutePath() + "-" + UUID.randomUUID());

    boolean result = true;
    try {
      result = FileUtils.copyDir(roleFolder, roleTmpSnapshotDir);
      result &= roleTmpSnapshotDir.renameTo(roleSnapshotDir);
    } finally {
      if (roleTmpSnapshotDir.exists() && !roleTmpSnapshotDir.delete()) {
        FileUtils.deleteDirectory(roleTmpSnapshotDir);
      }
    }
    return result;
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    SystemFileFactory systemFileFactory = SystemFileFactory.INSTANCE;
    File roleFolder = systemFileFactory.getFile(roleDirPath);
    File roleTmpFolder =
        systemFileFactory.getFile(roleFolder.getAbsolutePath() + "-" + UUID.randomUUID());
    File roleSnapshotDir = systemFileFactory.getFile(snapshotDir, ROLE_SNAPSHOT_FILE_NAME);
    if (roleSnapshotDir.exists()) {
      try {
        org.apache.commons.io.FileUtils.moveDirectory(roleFolder, roleTmpFolder);
        if (!FileUtils.copyDir(roleSnapshotDir, roleFolder)) {
          LOGGER.error("Failed to load role folder snapshot and rollback.");
          // rollback if failed to copy
          FileUtils.deleteDirectory(roleFolder);
          org.apache.commons.io.FileUtils.moveDirectory(roleTmpFolder, roleFolder);
        }
      } finally {
        FileUtils.deleteDirectory(roleTmpFolder);
      }
    } else {
      LOGGER.info("There are no roles to load.");
    }
  }

  @Override
  public void reset() {
    if (SystemFileFactory.INSTANCE.getFile(roleDirPath).mkdirs()) {
      LOGGER.info("role info dir {} is created", roleDirPath);
    } else if (!SystemFileFactory.INSTANCE.getFile(roleDirPath).exists()) {
      LOGGER.error("role info dir {} can not be created", roleDirPath);
    }
  }

  @Override
  public void cleanRoleFolder() {
    File[] files = SystemFileFactory.INSTANCE.getFile(roleDirPath).listFiles();
    for (File file : files) {
      FileUtils.deleteFileIfExist(file);
    }
  }

  @TestOnly
  public void saveRoleOldVer(Role role) throws IOException {
    File roleProfile =
        SystemFileFactory.INSTANCE.getFile(
            roleDirPath
                + File.separator
                + role.getName()
                + IoTDBConstant.PROFILE_SUFFIX
                + TEMP_SUFFIX);
    File roleDir = new File(roleDirPath);
    if (!roleDir.exists()) {
      if (!roleDir.mkdirs()) {
        LOGGER.error("Failed to create role dir {}", roleDirPath);
      }
    }
    try (BufferedOutputStream outputStream =
        new BufferedOutputStream(Files.newOutputStream(roleProfile.toPath()))) {
      try {
        IOUtils.writeString(outputStream, role.getName(), STRING_ENCODING, encodingBufferLocal);
        int privilegeNum = role.getPathPrivilegeList().size();
        IOUtils.writeInt(outputStream, privilegeNum, encodingBufferLocal);
        for (int i = 0; i < privilegeNum; i++) {
          PathPrivilege pathPrivilege = role.getPathPrivilegeList().get(i);
          IOUtils.writeString(
              outputStream,
              pathPrivilege.getPath().getFullPath(),
              STRING_ENCODING,
              encodingBufferLocal);
          IOUtils.writeInt(outputStream, pathPrivilege.getPrivileges().size(), encodingBufferLocal);
          for (Integer item : pathPrivilege.getPrivileges()) {
            IOUtils.writeInt(outputStream, item, encodingBufferLocal);
          }
        }
        outputStream.flush();
      } catch (Exception e) {
        throw new IOException(e);
      }
    } finally {
      encodingBufferLocal.remove();
    }

    File oldFile =
        SystemFileFactory.INSTANCE.getFile(
            roleDirPath + File.separator + role.getName() + IoTDBConstant.PROFILE_SUFFIX);
    IOUtils.replaceFile(roleProfile, oldFile);
  }
}
