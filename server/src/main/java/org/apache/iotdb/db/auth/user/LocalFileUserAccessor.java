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
package org.apache.iotdb.db.auth.user;

import org.apache.iotdb.db.auth.entity.PathPrivilege;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class loads a user's information from the corresponding file.The user file is a sequential
 * file. User file schema: Int32 username bytes size Utf-8 username bytes Int32 Password bytes size
 * Utf-8 password bytes Int32 seriesPath privilege number n Int32 seriesPath[1] size Utf-8
 * seriesPath[1] bytes Int32 privilege num k1 Int32 privilege[1][1] Int32 privilege[1][2] ... Int32
 * privilege[1][k1] Int32 seriesPath[2] size Utf-8 seriesPath[2] bytes Int32 privilege num k2 Int32
 * privilege[2][1] Int32 privilege[2][2] ... Int32 privilege[2][k2] ... Int32 seriesPath[n] size
 * Utf-8 seriesPath[n] bytes Int32 privilege num kn Int32 privilege[n][1] Int32 privilege[n][2] ...
 * Int32 privilege[n][kn] Int32 user name number m Int32 user name[1] size Utf-8 user name[1] bytes
 * Int32 user name[2] size Utf-8 user name[2] bytes ... Int32 user name[m] size Utf-8 user name[m]
 * bytes
 */
public class LocalFileUserAccessor implements IUserAccessor {

  private static final String TEMP_SUFFIX = ".temp";
  private static final String STRING_ENCODING = "utf-8";
  private static final Logger logger = LoggerFactory.getLogger(LocalFileUserAccessor.class);

  private String userDirPath;
  /**
   * Reused buffer for primitive types encoding/decoding, which aim to reduce memory fragments. Use
   * ThreadLocal for thread safety.
   */
  private ThreadLocal<ByteBuffer> encodingBufferLocal = new ThreadLocal<>();

  private ThreadLocal<byte[]> strBufferLocal = new ThreadLocal<>();

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
          logger.error("New profile renaming not succeed.");
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
      user.setName(IOUtils.readString(dataInputStream, STRING_ENCODING, strBufferLocal));
      user.setPassword(IOUtils.readString(dataInputStream, STRING_ENCODING, strBufferLocal));
      int privilegeNum = dataInputStream.readInt();
      List<PathPrivilege> pathPrivilegeList = new ArrayList<>();
      for (int i = 0; i < privilegeNum; i++) {
        pathPrivilegeList.add(
            IOUtils.readPathPrivilege(dataInputStream, STRING_ENCODING, strBufferLocal));
      }
      user.setPrivilegeList(pathPrivilegeList);
      int roleNum = dataInputStream.readInt();
      List<String> roleList = new ArrayList<>();
      for (int i = 0; i < roleNum; i++) {
        String userName = IOUtils.readString(dataInputStream, STRING_ENCODING, strBufferLocal);
        roleList.add(userName);
      }
      user.setRoleList(roleList);

      // for online upgrading, profile for v0.9.x/v1 does not contain waterMark
      long userProfileLength = userProfile.length();
      try {
        user.setUseWaterMark(dataInputStream.readInt() != 0);
      } catch (EOFException e1) {
        user.setUseWaterMark(false);
        try (RandomAccessFile file = new RandomAccessFile(userProfile, "rw")) {
          file.seek(userProfileLength);
          file.writeInt(0);
        }
      }
      return user;
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
        new BufferedOutputStream(new FileOutputStream(userProfile))) {
      try {
        IOUtils.writeString(outputStream, user.getName(), STRING_ENCODING, encodingBufferLocal);
        IOUtils.writeString(outputStream, user.getPassword(), STRING_ENCODING, encodingBufferLocal);

        user.getPrivilegeList().sort(PathPrivilege.REFERENCE_DESCENT_SORTER);
        int privilegeNum = user.getPrivilegeList().size();
        IOUtils.writeInt(outputStream, privilegeNum, encodingBufferLocal);
        for (int i = 0; i < privilegeNum; i++) {
          PathPrivilege pathPrivilege = user.getPrivilegeList().get(i);
          IOUtils.writePathPrivilege(
              outputStream, pathPrivilege, STRING_ENCODING, encodingBufferLocal);
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
    if (!userProfile.exists() && !backFile.exists()) {
      return false;
    }
    if ((userProfile.exists() && !userProfile.delete())
        || (backFile.exists() && !backFile.delete())) {
      throw new IOException(String.format("Cannot delete user file of %s", username));
    }
    return true;
  }

  @Override
  public List<String> listAllUsers() {
    File userDir = SystemFileFactory.INSTANCE.getFile(userDirPath);
    String[] names =
        userDir.list(
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
  public void reset() {
    if (SystemFileFactory.INSTANCE.getFile(userDirPath).mkdirs()) {
      logger.info("user info dir {} is created", userDirPath);
    } else if (!SystemFileFactory.INSTANCE.getFile(userDirPath).exists()) {
      logger.error("user info dir {} can not be created", userDirPath);
    }
  }
}
