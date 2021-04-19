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
package org.apache.iotdb.db.auth.role;

import org.apache.iotdb.db.auth.entity.PathPrivilege;
import org.apache.iotdb.db.auth.entity.Role;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.utils.IOUtils;

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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class store each role in a separate sequential file. Role file schema : Int32 role name size
 * Utf-8 role name bytes Int32 seriesPath privilege number n Int32 seriesPath[1] size Utf-8
 * seriesPath[1] bytes Int32 privilege num k1 Int32 privilege[1][1] Int32 privilege[1][2] ... Int32
 * privilege[1][k1] Int32 seriesPath[2] size Utf-8 seriesPath[2] bytes Int32 privilege num yk2 Int32
 * privilege[2][1] Int32 privilege[2][2] ... Int32 privilege[2][k2] ... Int32 seriesPath[n] size
 * Utf-8 seriesPath[n] bytes Int32 privilege num kn Int32 privilege[n][1] Int32 privilege[n][2] ...
 * Int32 privilege[n][kn]
 */
public class LocalFileRoleAccessor implements IRoleAccessor {
  private static final Logger logger = LoggerFactory.getLogger(LocalFileRoleAccessor.class);
  private static final String TEMP_SUFFIX = ".temp";
  private static final String STRING_ENCODING = "utf-8";

  private String roleDirPath;

  /**
   * Reused buffer for primitive types encoding/decoding, which aim to reduce memory fragments. Use
   * ThreadLocal for thread safety.
   */
  private ThreadLocal<ByteBuffer> encodingBufferLocal = new ThreadLocal<>();

  private ThreadLocal<byte[]> strBufferLocal = new ThreadLocal<>();

  public LocalFileRoleAccessor(String roleDirPath) {
    this.roleDirPath = roleDirPath;
  }

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
      role.setName(IOUtils.readString(dataInputStream, STRING_ENCODING, strBufferLocal));

      int privilegeNum = dataInputStream.readInt();
      List<PathPrivilege> pathPrivilegeList = new ArrayList<>();
      for (int i = 0; i < privilegeNum; i++) {
        pathPrivilegeList.add(
            IOUtils.readPathPrivilege(dataInputStream, STRING_ENCODING, strBufferLocal));
      }
      role.setPrivilegeList(pathPrivilegeList);

      return role;
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
    try (BufferedOutputStream outputStream =
        new BufferedOutputStream(new FileOutputStream(roleProfile))) {
      try {
        IOUtils.writeString(outputStream, role.getName(), STRING_ENCODING, encodingBufferLocal);

        role.getPrivilegeList().sort(PathPrivilege.REFERENCE_DESCENT_SORTER);
        int privilegeNum = role.getPrivilegeList().size();
        IOUtils.writeInt(outputStream, privilegeNum, encodingBufferLocal);
        for (int i = 0; i < privilegeNum; i++) {
          PathPrivilege pathPrivilege = role.getPrivilegeList().get(i);
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
  public void reset() {
    if (SystemFileFactory.INSTANCE.getFile(roleDirPath).mkdirs()) {
      logger.info("role info dir {} is created", roleDirPath);
    } else if (!SystemFileFactory.INSTANCE.getFile(roleDirPath).exists()) {
      logger.error("role info dir {} can not be created", roleDirPath);
    }
  }
}
