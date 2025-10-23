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
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.auth.role.LocalFileRoleAccessor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.IOUtils;
import org.apache.iotdb.commons.utils.TestOnly;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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
public class LocalFileUserAccessor extends LocalFileRoleAccessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileUserAccessor.class);
  public static final String ROLE_SUFFIX = "_role";

  public LocalFileUserAccessor(String userDirPath) {
    super(userDirPath);
  }

  @Override
  protected String getEntitySnapshotFileName() {
    return "system" + File.separator + "users";
  }

  @Override
  protected void saveEntityName(BufferedOutputStream outputStream, Role role) throws IOException {
    IOUtils.writeLong(outputStream, ((User) role).getUserId(), encodingBufferLocal);
    super.saveEntityName(outputStream, role);
    IOUtils.writeString(
        outputStream, ((User) role).getPassword(), STRING_ENCODING, encodingBufferLocal);
  }

  @Override
  protected void saveSessionPerUser(BufferedOutputStream outputStream, Role role)
      throws IOException {
    IOUtils.writeInt(outputStream, role.getMaxSessionPerUser(), encodingBufferLocal);
    IOUtils.writeInt(outputStream, role.getMinSessionPerUser(), encodingBufferLocal);
  }

  @Override
  protected void saveRoles(Role role) throws IOException {
    User user = (User) role;
    File roleProfile =
        SystemFileFactory.INSTANCE.getFile(
            entityDirPath
                + File.separator
                + user.getUserId()
                + ROLE_SUFFIX
                + IoTDBConstant.PROFILE_SUFFIX
                + TEMP_SUFFIX);
    try (FileOutputStream fileOutputStream = new FileOutputStream(roleProfile);
        BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream)) {
      for (String roleName : user.getRoleSet()) {
        IOUtils.writeString(outputStream, roleName, STRING_ENCODING, encodingBufferLocal);
      }
      outputStream.flush();
      fileOutputStream.getFD().sync();
    } catch (Exception e) {
      LOGGER.warn("Get Exception when save user {}'s roles", user.getName(), e);
      throw new IOException(e);
    } finally {
      encodingBufferLocal.remove();
    }

    File oldURoleFile =
        SystemFileFactory.INSTANCE.getFile(
            entityDirPath
                + File.separator
                + user.getUserId()
                + ROLE_SUFFIX
                + IoTDBConstant.PROFILE_SUFFIX);
    IOUtils.replaceFile(roleProfile, oldURoleFile);
  }

  /**
   * Deserialize a user from its user file.
   *
   * @param entityName The name of the user to be deserialized.
   * @return The user object or null if no such user.
   */
  @Override
  public User loadEntity(String entityName) throws IOException {
    File entityFile = checkFileAvailable(entityName, "");
    if (entityFile == null) {
      return null;
    }
    FileInputStream inputStream = new FileInputStream(entityFile);
    try (DataInputStream dataInputStream =
        new DataInputStream(new BufferedInputStream(inputStream))) {
      int tag = dataInputStream.readInt();
      User user = new User();
      if (tag < 0) {
        String name =
            IOUtils.readString(dataInputStream, STRING_ENCODING, strBufferLocal, -1 * tag);
        user.setName(name);
        user.setPassword(IOUtils.readString(dataInputStream, STRING_ENCODING, strBufferLocal));
        user.setSysPrivilegesWithMask(dataInputStream.readInt());
        List<PathPrivilege> pathPrivilegeList = new ArrayList<>();
        for (int i = 0; dataInputStream.available() != 0; i++) {
          pathPrivilegeList.add(
              IOUtils.readPathPrivilege(dataInputStream, STRING_ENCODING, strBufferLocal));
        }
        user.setPrivilegeList(pathPrivilegeList);
      } else if (tag == 1) {
        user.setName(IOUtils.readString(dataInputStream, STRING_ENCODING, strBufferLocal));
        user.setPassword(IOUtils.readString(dataInputStream, STRING_ENCODING, strBufferLocal));
        loadPrivileges(dataInputStream, user);
      } else {
        assert (tag == VERSION);
        user.setUserId(dataInputStream.readLong());
        user.setName(IOUtils.readString(dataInputStream, STRING_ENCODING, strBufferLocal));
        user.setPassword(IOUtils.readString(dataInputStream, STRING_ENCODING, strBufferLocal));
        user.setMaxSessionPerUser(dataInputStream.readInt());
        user.setMinSessionPerUser(dataInputStream.readInt());
        loadPrivileges(dataInputStream, user);
      }

      File roleOfUser = checkFileAvailable(entityName, "_role");
      Set<String> roleSet = new HashSet<>();
      if (roleOfUser != null && roleOfUser.exists()) {
        inputStream = new FileInputStream(roleOfUser);
        try (DataInputStream roleInputStream =
            new DataInputStream(new BufferedInputStream(inputStream))) {
          for (int i = 0; roleInputStream.available() != 0; i++) {
            String roleName = IOUtils.readString(roleInputStream, STRING_ENCODING, strBufferLocal);
            roleSet.add(roleName);
          }
        }
      }
      user.setRoleSet(roleSet);
      return user;

    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      strBufferLocal.remove();
    }
  }

  /**
   * Delete a user's user file.
   *
   * @param entityName The name of the user to be deleted.
   * @return True if the file is successfully deleted, false if the file does not exist.
   * @throws IOException when the file cannot be deleted.
   */
  @Override
  public boolean deleteEntity(String entityName) throws IOException {
    return super.deleteEntity(entityName) && deleteURole(entityName);
  }

  private boolean deleteURole(String username) throws IOException {
    File uRoleProfile =
        SystemFileFactory.INSTANCE.getFile(
            entityDirPath + File.separator + username + ROLE_SUFFIX + IoTDBConstant.PROFILE_SUFFIX);
    File backProfile =
        SystemFileFactory.INSTANCE.getFile(
            entityDirPath
                + File.separator
                + username
                + ROLE_SUFFIX
                + IoTDBConstant.PROFILE_SUFFIX
                + TEMP_SUFFIX);
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
  public List<String> listAllEntities() {
    File userDir = SystemFileFactory.INSTANCE.getFile(entityDirPath);
    String[] names =
        userDir.list(
            (dir, name) ->
                (name.endsWith(IoTDBConstant.PROFILE_SUFFIX)
                        && !name.endsWith(ROLE_SUFFIX + IoTDBConstant.PROFILE_SUFFIX))
                    || (name.endsWith(TEMP_SUFFIX)
                        && !name.endsWith(
                            ROLE_SUFFIX + IoTDBConstant.PROFILE_SUFFIX + TEMP_SUFFIX)));
    return getEntityStrings(names);
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    SystemFileFactory systemFileFactory = SystemFileFactory.INSTANCE;
    File userFolder = systemFileFactory.getFile(entityDirPath);
    File userTmpFolder =
        systemFileFactory.getFile(userFolder.getAbsolutePath() + "-" + UUID.randomUUID());
    File userSnapshotDir = systemFileFactory.getFile(snapshotDir, getEntitySnapshotFileName());

    try {
      org.apache.tsfile.external.commons.io.FileUtils.moveDirectory(userFolder, userTmpFolder);
      if (!FileUtils.copyDir(userSnapshotDir, userFolder)) {
        LOGGER.error("Failed to load user folder snapshot and rollback.");
        // rollback if failed to copy
        FileUtils.deleteFileOrDirectory(userFolder);
        org.apache.tsfile.external.commons.io.FileUtils.moveDirectory(userTmpFolder, userFolder);
      }
    } finally {
      FileUtils.deleteFileOrDirectory(userTmpFolder);
    }
  }

  @TestOnly
  public void saveUserOldVersion(User user) throws IOException {
    File userProfile =
        SystemFileFactory.INSTANCE.getFile(
            entityDirPath
                + File.separator
                + user.getName()
                + IoTDBConstant.PROFILE_SUFFIX
                + TEMP_SUFFIX);

    try (FileOutputStream fileOutputStream = new FileOutputStream(userProfile);
        BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream)) {
      // for IoTDB 1.3, the username's length will be stored as a negative number.
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
      fileOutputStream.getFD().sync();

    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      encodingBufferLocal.remove();
    }

    File oldFile =
        SystemFileFactory.INSTANCE.getFile(
            entityDirPath + File.separator + user.getName() + IoTDBConstant.PROFILE_SUFFIX);
    IOUtils.replaceFile(userProfile, oldFile);
  }

  @TestOnly
  public void saveUserOldVersion1(User user) throws IOException {
    File userProfile =
        SystemFileFactory.INSTANCE.getFile(
            entityDirPath
                + File.separator
                + user.getName()
                + IoTDBConstant.PROFILE_SUFFIX
                + TEMP_SUFFIX);

    try (FileOutputStream fileOutputStream = new FileOutputStream(userProfile);
        BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream)) {
      // test for version1
      IOUtils.writeInt(outputStream, 1, encodingBufferLocal);
      IOUtils.writeString(outputStream, user.getName(), STRING_ENCODING, encodingBufferLocal);
      IOUtils.writeString(outputStream, user.getPassword(), STRING_ENCODING, encodingBufferLocal);
      savePrivileges(outputStream, user);
      outputStream.flush();
      fileOutputStream.getFD().sync();
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      encodingBufferLocal.remove();
    }

    File oldFile =
        SystemFileFactory.INSTANCE.getFile(
            entityDirPath + File.separator + user.getName() + IoTDBConstant.PROFILE_SUFFIX);
    IOUtils.replaceFile(userProfile, oldFile);
  }
}
