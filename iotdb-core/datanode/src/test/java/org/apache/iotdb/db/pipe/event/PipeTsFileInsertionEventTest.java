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

package org.apache.iotdb.db.pipe.event;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckContext;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern.buildUnionPattern;
import static org.apache.iotdb.db.auth.AuthorityChecker.NO_PERMISSION_PROMOTION;

public class PipeTsFileInsertionEventTest {
  @Test
  public void testAuthCheck() throws Exception {
    final AccessControl oldControl = AuthorityChecker.getAccessControl();
    final File file =
        new File(
            TsFileNameGenerator.generateNewTsFilePath(
                TestConstant.BASE_OUTPUT_PATH + IoTDBConstant.SEQUENCE_FOLDER_NAME, 1, 1, 1, 1));
    try {
      AuthorityChecker.setAccessControl(new TestAccessControl());

      final TsFileResource resource = new TsFileResource(file);
      try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
        writer.startChunkGroup("d1");
        writer.generateSimpleAlignedSeriesToCurrentDevice(
            Arrays.asList("s1", "s2"),
            new TimeRange[][][] {
              new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12), new TimeRange(3, 12)}}
            },
            TSEncoding.PLAIN,
            CompressionType.LZ4);
        writer.endChunkGroup();
        writer.endFile();
      }
      resource.setStatus(TsFileResourceStatus.NORMAL);

      final ITimeIndex timeIndex = new ArrayDeviceTimeIndex();
      final IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.d1");
      timeIndex.putStartTime(deviceID, 0);
      timeIndex.putEndTime(deviceID, 1);
      resource.setTimeIndex(timeIndex);

      final PipeTsFileInsertionEvent tableEvent =
          new PipeTsFileInsertionEvent(
              true,
              "db",
              resource,
              null,
              true,
              false,
              false,
              Collections.singleton("table"),
              null,
              0,
              null,
              buildUnionPattern(
                  false, Collections.singletonList(new IoTDBTreePattern(false, null))),
              new TablePattern(true, null, null),
              "0",
              "user",
              "localhost",
              false,
              Long.MIN_VALUE,
              Long.MAX_VALUE);
      Assert.assertThrows(AccessDeniedException.class, tableEvent::throwIfNoPrivilege);
      tableEvent.close();

      final PipeTsFileInsertionEvent treeEvent =
          new PipeTsFileInsertionEvent(
              false,
              "root.db",
              resource,
              null,
              true,
              false,
              false,
              null,
              null,
              0,
              null,
              buildUnionPattern(true, Collections.singletonList(new IoTDBTreePattern(true, null))),
              new TablePattern(false, null, null),
              "0",
              "user",
              "localhost",
              false,
              Long.MIN_VALUE,
              Long.MAX_VALUE);
      // Shall not throw any exceptions for historical files
      treeEvent.throwIfNoPrivilege();
      Assert.assertTrue(treeEvent.shouldParse4Privilege());

      treeEvent.setTreeSchemaMap(Collections.singletonMap(deviceID, new String[] {"s0", "s1"}));
      Assert.assertThrows(AccessDeniedException.class, treeEvent::throwIfNoPrivilege);

      treeEvent.close();
    } finally {
      AuthorityChecker.setAccessControl(oldControl);
      FileUtils.deleteFileOrDirectory(new File(TestConstant.BASE_OUTPUT_PATH));
    }
  }

  static class TestAccessControl implements AccessControl {

    @Override
    public void checkCanCreateDatabase(
        String userName, String databaseName, IAuditEntity auditEntity) {}

    @Override
    public void checkCanDropDatabase(
        String userName, String databaseName, IAuditEntity auditEntity) {}

    @Override
    public void checkCanAlterDatabase(
        String userName, String databaseName, IAuditEntity auditEntity) {}

    @Override
    public void checkCanShowOrUseDatabase(
        String userName, String databaseName, IAuditEntity auditEntity) {}

    @Override
    public void checkCanCreateTable(
        String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {}

    @Override
    public void checkCanDropTable(
        String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {}

    @Override
    public void checkCanAlterTable(
        String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {}

    @Override
    public void checkCanInsertIntoTable(
        String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {}

    @Override
    public void checkCanSelectFromTable(
        String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {
      throw new AccessDeniedException(
          NO_PERMISSION_PROMOTION
              + PrivilegeType.SELECT
              + " ON "
              + tableName.getDatabaseName()
              + "."
              + tableName.getObjectName());
    }

    @Override
    public void checkCanSelectFromDatabase4Pipe(
        String userName, String databaseName, IAuditEntity auditEntity) {
      throw new AccessDeniedException(
          NO_PERMISSION_PROMOTION + PrivilegeType.SELECT + " ON DB:" + databaseName);
    }

    @Override
    public boolean checkCanSelectFromTable4Pipe(
        String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {
      return false;
    }

    @Override
    public void checkCanDeleteFromTable(
        String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {}

    @Override
    public void checkCanShowOrDescTable(
        String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {}

    @Override
    public void checkCanCreateViewFromTreePath(PartialPath path, IAuditEntity auditEntity) {}

    @Override
    public void checkUserCanRunRelationalAuthorStatement(
        String userName, RelationalAuthorStatement statement, IAuditEntity auditEntity) {}

    @Override
    public void checkUserIsAdmin(IAuditEntity auditEntity) {}

    @Override
    public void checkUserGlobalSysPrivilege(IAuditEntity auditEntity) {}

    @Override
    public boolean hasGlobalPrivilege(IAuditEntity auditEntity, PrivilegeType privilegeType) {
      return false;
    }

    @Override
    public void checkMissingPrivileges(
        String username, Collection<PrivilegeType> privilegeTypes, IAuditEntity auditEntity) {}

    @Override
    public TSStatus checkPermissionBeforeProcess(
        Statement statement, TreeAccessCheckContext context) {
      return null;
    }

    @Override
    public TSStatus checkFullPathWriteDataPermission(
        IAuditEntity auditEntity, IDeviceID device, String measurementId) {
      return null;
    }

    @Override
    public TSStatus checkCanCreateDatabaseForTree(IAuditEntity entity, PartialPath databaseName) {
      return null;
    }

    @Override
    public TSStatus checkCanAlterTemplate(IAuditEntity entity, Supplier<String> auditObject) {
      return null;
    }

    @Override
    public TSStatus checkCanAlterView(
        IAuditEntity entity, List<PartialPath> sourcePaths, List<PartialPath> targetPaths) {
      return null;
    }

    @Override
    public TSStatus checkSeriesPrivilege4Pipe(
        IAuditEntity context,
        List<? extends PartialPath> checkedPathsSupplier,
        PrivilegeType permission) {
      return AuthorityChecker.getTSStatus(
          IntStream.range(0, checkedPathsSupplier.size()).boxed().collect(Collectors.toList()),
          checkedPathsSupplier,
          permission);
    }

    @Override
    public List<Integer> checkSeriesPrivilegeWithIndexes4Pipe(
        IAuditEntity context,
        List<? extends PartialPath> checkedPathsSupplier,
        PrivilegeType permission) {
      return null;
    }

    @Override
    public TSStatus allowUserToLogin(String userName) {
      return null;
    }
  }
}
