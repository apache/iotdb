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
package org.apache.iotdb.commons.auth.user;

import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.User;
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
import java.io.EOFException;
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
 * This class store user info in separate sequential files and every user will have one user file
 * and an optional user_role file. user file schema: username-length int32| username-bytes-utf8|
 * userpwd-length int32 | userpwd-bytes-utf8| system-perivileges int32| path[1]-length int32|
 * path[1]-bytes-utf8 | privilege-int32| path[2]-length int32| path[2]-bytes-utf8 | privilege-int32|
 * path[3]-length int32| path[3]-bytes-utf8 | privilege-int32|
 *
 * <p>user_role file schema: role1-length int32 | role1-bytes-utf8 | role2-length int32 |
 * role2-bytes-utf8... .....
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
 * <p>user files and user_role files are appendable.
 */

/**
 * All user/role file store in config node's user/role folder. when our system start, we load all
 * user/role from folder. If we did some alter query, we just store raft log.
 */
public class LocalFileUserAccessor implements IUserAccessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileUserAccessor.class);
  private static final String TEMP_SUFFIX = ".temp";
  private static final String STRING_ENCODING = "utf-8";
  private static final String USER_SNAPSHOT_FILE_NAME = "system" + File.separator + "users";

  private static final String ROLE_SUFFIX = "_role";

  private final String userDirPath;
  /**
   * Reused buffer for primitive types encoding/decoding, which aim to reduce memory fragments. Use
   * ThreadLocal for thread safety.
   */
  private final ThreadLocal<ByteBuffer> encodingBufferLocal = new ThreadLocal<>();

  private final ThreadLocal<byte[]> strBufferLocal = new ThreadLocal<>();

  public LocalFileUserAccessor(String userDirPath) {
    this.userDirPath = userDirPath;
  }

  /**
   * Deserialize a user from its user file.
   *
   * @param username The name of the user to be deserialized.
   * @return The user object or null if no such user.
   */
  @Override
  public User loadUser(String username) throws IOException {
    File userProfile =
        SystemFileFactory.INSTANCE.getFile(
            userDirPath + File.separator + username + IoTDBConstant.PROFILE_SUFFIX);
    if (!userProfile.exists() || !userProfile.isFile()) {
      // System may crush before a newer file is renamed.
      File newProfile =
          SystemFileFactory.INSTANCE.getFile(
              userDirPath + File.separator + username + IoTDBConstant.PROFILE_SUFFIX + TEMP_SUFFIX);
      if (newProfile.exists() && newProfile.isFile()) {
        if (!newProfile.renameTo(userProfile)) {
          LOGGER.error("New profile renaming not succeed.");
        }
        userProfile = newProfile;
      } else {
        return null;
      }
    }

    FileInputStream inputStream = new FileInputStream(userProfile);
    try (DataInputStream dataInputStream =
        new DataInputStream(new BufferedInputStream(inputStream))) {
      User user = new User();
      Pair<String, Boolean> result =
          IOUtils.readAuthString(dataInputStream, STRING_ENCODING, strBufferLocal);
      boolean oldVersion = result.getRight();
      user.setName(result.getLeft());
      user.setPassword(IOUtils.readString(dataInputStream, STRING_ENCODING, strBufferLocal));
      if (oldVersion) {
        IOUtils.loadRolePrivilege(user, dataInputStream, STRING_ENCODING, strBufferLocal);
        int roleNum = dataInputStream.readInt();
        List<String> roleList = new ArrayList<>();
        for (int i = 0; i < roleNum; i++) {
          String roleName = IOUtils.readString(dataInputStream, STRING_ENCODING, strBufferLocal);
          roleList.add(roleName);
        }
        user.setRoleList(roleList);
        try {
          user.setUseWaterMark(dataInputStream.readInt() != 0);
        } catch (EOFException e1) {
          user.setUseWaterMark(false);
        }
        return user;
      } else {
        user.setSysPrivilegeSet(dataInputStream.readInt());
        List<PathPrivilege> pathPrivilegeList = new ArrayList<>();
        for (int i = 0; dataInputStream.available() != 0; i++) {
          pathPrivilegeList.add(
              IOUtils.readPathPrivilege(dataInputStream, STRING_ENCODING, strBufferLocal, false));
        }
        user.setPrivilegeList(pathPrivilegeList);

        File roleOfUser =
            SystemFileFactory.INSTANCE.getFile(
                userDirPath,
                File.separator + username + ROLE_SUFFIX + IoTDBConstant.PROFILE_SUFFIX);

        if (!roleOfUser.isFile() || !roleOfUser.exists()) {
          // System may crush before a newer file is renamed.
          File newRoleProfile =
              SystemFileFactory.INSTANCE.getFile(
                  userDirPath
                      + File.separator
                      + username
                      + "_role"
                      + IoTDBConstant.PROFILE_SUFFIX
                      + TEMP_SUFFIX);
          if (newRoleProfile.exists() && newRoleProfile.isFile()) {
            if (!newRoleProfile.renameTo(roleOfUser)) {
              LOGGER.warn(" New role profile renaming not succeed.");
            }
            roleOfUser = newRoleProfile;
          }
        }

        List<String> roleList = new ArrayList<>();
        if (roleOfUser.exists()) {
          inputStream = new FileInputStream(roleOfUser);
          try (DataInputStream roleInpuStream =
              new DataInputStream(new BufferedInputStream(inputStream))) {

            for (int i = 0; roleInpuStream.available() != 0; i++) {
              String rolename = IOUtils.readString(roleInpuStream, STRING_ENCODING, strBufferLocal);
              roleList.add(rolename);
            }
          }
        }
        user.setRoleList(roleList);
        return user;
      }
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      strBufferLocal.remove();
    }
  }

  /**
   * Serialize the user object to a temp file, then replace the old user file with the new file.
   *
   * @param user The user object that is to be saved.
   */
  @Override
  public void saveUser(User user) throws IOException {
    File userProfile =
        SystemFileFactory.INSTANCE.getFile(
            userDirPath
                + File.separator
                + user.getName()
                + IoTDBConstant.PROFILE_SUFFIX
                + TEMP_SUFFIX);

    try (BufferedOutputStream outputStream =
        new BufferedOutputStream(Files.newOutputStream(userProfile.toPath()))) {
      try {
        // for IOTDB 1.2, the username's length will be stored as a negative number.
        byte[] strBuffer = user.getName().getBytes(STRING_ENCODING);
        IOUtils.writeInt(outputStream, -1 * strBuffer.length, encodingBufferLocal);
        outputStream.write(strBuffer);
        IOUtils.writeString(outputStream, user.getPassword(), STRING_ENCODING, encodingBufferLocal);
        IOUtils.writeInt(outputStream, user.getAllSysPrivileges(), encodingBufferLocal);

        int privilegeNum = user.getPathPrivilegeList().size();
        for (int i = 0; i < privilegeNum; i++) {
          PathPrivilege pathPrivilege = user.getPathPrivilegeList().get(i);
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

    File roleProfile =
        SystemFileFactory.INSTANCE.getFile(
            userDirPath
                + File.separator
                + user.getName()
                + ROLE_SUFFIX
                + IoTDBConstant.PROFILE_SUFFIX
                + TEMP_SUFFIX);
    try (BufferedOutputStream roleOutputStream =
        new BufferedOutputStream(Files.newOutputStream(roleProfile.toPath()))) {
      try {
        int userNum = user.getRoleList().size();
        for (int i = 0; i < userNum; i++) {
          IOUtils.writeString(
              roleOutputStream, user.getRoleList().get(i), STRING_ENCODING, encodingBufferLocal);
        }
        roleOutputStream.flush();
      } catch (Exception e) {
        throw new IOException(e);
      }
    } finally {
      encodingBufferLocal.remove();
    }

    File oldUserFile =
        SystemFileFactory.INSTANCE.getFile(
            userDirPath + File.separator + user.getName() + IoTDBConstant.PROFILE_SUFFIX);
    IOUtils.replaceFile(userProfile, oldUserFile);
    File oldURoleFile =
        SystemFileFactory.INSTANCE.getFile(
            userDirPath
                + File.separator
                + user.getName()
                + ROLE_SUFFIX
                + IoTDBConstant.PROFILE_SUFFIX);
    IOUtils.replaceFile(roleProfile, oldURoleFile);
  }

  /**
   * Delete a user's user file.
   *
   * @param username The name of the user to be deleted.
   * @return True if the file is successfully deleted, false if the file does not exists.
   * @throws IOException when the file cannot be deleted.
   */
  @Override
  public boolean deleteUser(String username) throws IOException {
    File userProfile =
        SystemFileFactory.INSTANCE.getFile(
            userDirPath + File.separator + username + IoTDBConstant.PROFILE_SUFFIX);
    File backFile =
        SystemFileFactory.INSTANCE.getFile(
            userDirPath + File.separator + username + IoTDBConstant.PROFILE_SUFFIX + TEMP_SUFFIX);
    // The user may be not flush. So if not find the file, just return true;
    if (!userProfile.exists() && !backFile.exists()) {
      return true;
    }
    if ((userProfile.exists() && !userProfile.delete())
        || (backFile.exists() && !backFile.delete())) {
      throw new IOException(String.format("Cannot delete user file of %s", username));
    }
    if (!deleteURole(username)) {
      return false;
    }
    return true;
  }

  public boolean deleteURole(String username) throws IOException {
    File uRoleProfile =
        SystemFileFactory.INSTANCE.getFile(
            userDirPath + File.separator + username + ROLE_SUFFIX + IoTDBConstant.PROFILE_SUFFIX);
    File backProfile =
        SystemFileFactory.INSTANCE.getFile(
            userDirPath
                + File.separator
                + username
                + ROLE_SUFFIX
                + IoTDBConstant.PROFILE_SUFFIX
                + TEMP_SUFFIX);
    // This role don't have any role.
    if (!uRoleProfile.exists() && !backProfile.exists()) {
      return true;
    }
    if ((uRoleProfile.exists() && !uRoleProfile.delete())
        || (backProfile.exists() && !backProfile.delete())) {
      throw new IOException(String.format("Catch error when delete %s 's role", username));
    }
    return true;
  }

  @Override
  public List<String> listAllUsers() {
    File userDir = SystemFileFactory.INSTANCE.getFile(userDirPath);
    String[] names =
        userDir.list(
            (dir, name) ->
                (name.endsWith(IoTDBConstant.PROFILE_SUFFIX)
                        && !name.endsWith(ROLE_SUFFIX + IoTDBConstant.PROFILE_SUFFIX))
                    || (name.endsWith(TEMP_SUFFIX) && !name.endsWith(ROLE_SUFFIX + TEMP_SUFFIX)));
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
    File userFolder = systemFileFactory.getFile(userDirPath);
    File userSnapshotDir = systemFileFactory.getFile(snapshotDir, USER_SNAPSHOT_FILE_NAME);
    File userTmpSnapshotDir =
        systemFileFactory.getFile(userSnapshotDir.getAbsolutePath() + "-" + UUID.randomUUID());

    boolean result = true;
    try {
      result = FileUtils.copyDir(userFolder, userTmpSnapshotDir);
      result &= userTmpSnapshotDir.renameTo(userSnapshotDir);
    } finally {
      if (userTmpSnapshotDir.exists() && !userTmpSnapshotDir.delete()) {
        FileUtils.deleteDirectory(userTmpSnapshotDir);
      }
    }
    return result;
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    SystemFileFactory systemFileFactory = SystemFileFactory.INSTANCE;
    File userFolder = systemFileFactory.getFile(userDirPath);
    File userTmpFolder =
        systemFileFactory.getFile(userFolder.getAbsolutePath() + "-" + UUID.randomUUID());
    File userSnapshotDir = systemFileFactory.getFile(snapshotDir, USER_SNAPSHOT_FILE_NAME);

    try {
      org.apache.commons.io.FileUtils.moveDirectory(userFolder, userTmpFolder);
      if (!FileUtils.copyDir(userSnapshotDir, userFolder)) {
        LOGGER.error("Failed to load user folder snapshot and rollback.");
        // rollback if failed to copy
        FileUtils.deleteDirectory(userFolder);
        org.apache.commons.io.FileUtils.moveDirectory(userTmpFolder, userFolder);
      }
    } finally {
      FileUtils.deleteDirectory(userTmpFolder);
    }
  }

  @Override
  public void reset() {
    if (SystemFileFactory.INSTANCE.getFile(userDirPath).mkdirs()) {
      LOGGER.info("user info dir {} is created", userDirPath);
    } else if (!SystemFileFactory.INSTANCE.getFile(userDirPath).exists()) {
      LOGGER.error("user info dir {} can not be created", userDirPath);
    }
  }

  @Override
  public String getDirPath() {
    return userDirPath;
  }

  @Override
  public void cleanUserFolder() {
    File[] files = SystemFileFactory.INSTANCE.getFile(userDirPath).listFiles();
    for (File file : files) {
      FileUtils.deleteFileIfExist(file);
    }
  }

  @TestOnly
  public void saveUserOldVersion(User user) throws IOException {
    File userProfile =
        SystemFileFactory.INSTANCE.getFile(
            userDirPath
                + File.separator
                + user.getName()
                + IoTDBConstant.PROFILE_SUFFIX
                + TEMP_SUFFIX);

    try (BufferedOutputStream outputStream =
        new BufferedOutputStream(Files.newOutputStream(userProfile.toPath()))) {
      try {
        IOUtils.writeString(outputStream, user.getName(), STRING_ENCODING, encodingBufferLocal);
        IOUtils.writeString(outputStream, user.getPassword(), STRING_ENCODING, encodingBufferLocal);

        int privilegeNum = user.getPathPrivilegeList().size();
        IOUtils.writeInt(outputStream, privilegeNum, encodingBufferLocal);
        for (int i = 0; i < privilegeNum; i++) {
          PathPrivilege pathPrivilege = user.getPathPrivilegeList().get(i);
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
        int userNum = user.getRoleList().size();
        IOUtils.writeInt(outputStream, userNum, encodingBufferLocal);
        for (int i = 0; i < userNum; i++) {
          IOUtils.writeString(
              outputStream, user.getRoleList().get(i), STRING_ENCODING, encodingBufferLocal);
        }
        IOUtils.writeInt(outputStream, user.isUseWaterMark() ? 1 : 0, encodingBufferLocal);
        outputStream.flush();

      } catch (Exception e) {
        throw new IOException(e);
      }
    } finally {
      encodingBufferLocal.remove();
    }

    File oldFile =
        SystemFileFactory.INSTANCE.getFile(
            userDirPath + File.separator + user.getName() + IoTDBConstant.PROFILE_SUFFIX);
    IOUtils.replaceFile(userProfile, oldFile);
  }
}
