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

package org.apache.iotdb.db.storageengine.load.converter;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.IAuthorCache;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckContext;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Method;
import java.time.ZoneId;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class LoadTsFileDataTypeConverterTest {

  private static final String USER_NAME = "load_converter_test_user";
  private static final long USER_ID = 10001L;
  private static final ZoneId ZONE_ID = ZoneId.of("Asia/Shanghai");

  private final SessionManager sessionManager = SessionManager.getInstance();
  private AccessControl previousAccessControl;
  private AccessControl accessControl;
  private IAuthorCache authorCache;
  private User previousCachedUser;

  @Before
  public void setUp() {
    Assert.assertNull(sessionManager.getCurrSession());
    previousAccessControl = AuthorityChecker.getAccessControl();
    accessControl = mock(AccessControl.class);
    AuthorityChecker.setAccessControl(accessControl);
    authorCache = AuthorityChecker.getAuthorityFetcher().getAuthorCache();
    previousCachedUser = authorCache.getUserCache(USER_NAME);
    authorCache.putUserCache(USER_NAME, new User(USER_NAME, "unused", USER_ID));
  }

  @After
  public void tearDown() {
    sessionManager.removeCurrSession();
    AuthorityChecker.setAccessControl(previousAccessControl);
    authorCache.invalidateCache(USER_NAME, null);
    if (previousCachedUser != null) {
      authorCache.putUserCache(USER_NAME, previousCachedUser);
    }
  }

  @Test
  public void testBackgroundTreeConversionChecksRestoredUserPermissions() throws Exception {
    final MPPQueryContext context = new MPPQueryContext(new QueryId("load_conversion_auth"));
    context.setSession(
        new SessionInfo(
            0,
            new UserEntity(USER_ID, USER_NAME, ""),
            ZONE_ID,
            IoTDBConstant.ClientVersion.V_1_0,
            null,
            SqlDialect.TREE));

    assertTreeConversionDenied(new LoadTsFileDataTypeConverter(context, true));
    Assert.assertNull(sessionManager.getCurrSession());
  }

  @Test
  public void testTreeConversionChecksCurrentSessionAndPreservesIt() throws Exception {
    final InternalClientSession session = new InternalClientSession("load-converter-test");
    session.setUserId(USER_ID);
    session.setUsername(USER_NAME);
    session.setZoneId(ZONE_ID);
    session.setClientVersion(IoTDBConstant.ClientVersion.V_1_0);
    sessionManager.registerSession(session);

    assertTreeConversionDenied(new LoadTsFileDataTypeConverter(null, false));
    Assert.assertSame(session, sessionManager.getCurrSession());
  }

  private void assertTreeConversionDenied(final LoadTsFileDataTypeConverter converter)
      throws Exception {
    final TSStatus denied = new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode());
    final Statement statement = new InsertMultiTabletsStatement();
    doAnswer(
            invocation -> {
              final IClientSession session = sessionManager.getCurrSession();
              Assert.assertNotNull(session);
              Assert.assertEquals(USER_NAME, session.getUsername());
              Assert.assertEquals(USER_ID, session.getUserId());
              Assert.assertEquals(ZONE_ID, session.getZoneId());
              return denied;
            })
        .when(accessControl)
        .checkPermissionBeforeProcess(same(statement), any(TreeAccessCheckContext.class));

    Assert.assertSame(denied, executeTree(converter, statement));
    final ArgumentCaptor<TreeAccessCheckContext> checkedUser =
        ArgumentCaptor.forClass(TreeAccessCheckContext.class);
    verify(accessControl).checkPermissionBeforeProcess(same(statement), checkedUser.capture());
    Assert.assertEquals(USER_NAME, checkedUser.getValue().getUsername());
    Assert.assertEquals(USER_ID, checkedUser.getValue().getUserId());
  }

  @Test
  public void testBackgroundConversionWithoutIdentityFailsClosed() throws Exception {
    final LoadTsFileDataTypeConverter converter = new LoadTsFileDataTypeConverter(null, true);
    Assert.assertEquals(
        TSStatusCode.NOT_LOGIN.getStatusCode(),
        executeTree(converter, new InsertMultiTabletsStatement()).getCode());
    final Method executeTable =
        LoadTsFileDataTypeConverter.class.getDeclaredMethod(
            "executeForTableModel", Statement.class, String.class);
    executeTable.setAccessible(true);
    Assert.assertEquals(
        TSStatusCode.NOT_LOGIN.getStatusCode(),
        ((TSStatus) executeTable.invoke(converter, new InsertTabletStatement(), "db")).getCode());
    Assert.assertNull(sessionManager.getCurrSession());
    verifyZeroInteractions(accessControl);
  }

  private static TSStatus executeTree(
      final LoadTsFileDataTypeConverter converter, final Statement statement) throws Exception {
    final Method executeTree =
        LoadTsFileDataTypeConverter.class.getDeclaredMethod("executeForTreeModel", Statement.class);
    executeTree.setAccessible(true);
    return (TSStatus) executeTree.invoke(converter, statement);
  }
}
