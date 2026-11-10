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

package org.apache.iotdb.it.utils;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.isession.INodeSupplier;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.rpc.UrlUtils;
import org.apache.iotdb.session.DummyNodesSupplier;
import org.apache.iotdb.session.Session;

import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.awaitility.Awaitility;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;

public final class InternalSessionITUtils {

  private static final String DATA_REGION = "DataRegion";
  private static final String LEADER = "Leader";

  private InternalSessionITUtils() {}

  public static TEndPoint getInternalEndPoint(final DataNodeWrapper dataNode) {
    return new TEndPoint(dataNode.getInternalAddress(), dataNode.getInternalPort());
  }

  public static String getEndPointUrl(final TEndPoint endPoint) {
    return UrlUtils.convertTEndPointIpv4AndIpv6Url(endPoint);
  }

  public static List<TEndPoint> getInternalEndPoints() {
    final List<TEndPoint> endPoints = new ArrayList<>();
    for (final DataNodeWrapper dataNode : EnvFactory.getEnv().getDataNodeWrapperList()) {
      endPoints.add(getInternalEndPoint(dataNode));
    }
    return endPoints;
  }

  public static INodeSupplier createInternalNodeSupplier() {
    return new DummyNodesSupplier(getInternalEndPoints());
  }

  public static TEndPoint findNonLeaderInternalEndPoint(
      final Set<TEndPoint> leaderInternalEndPoints) {
    for (final TEndPoint endPoint : getInternalEndPoints()) {
      if (!leaderInternalEndPoints.contains(endPoint)) {
        return endPoint;
      }
    }
    throw new IllegalStateException("No non-leader DataNode is available");
  }

  public static Set<TEndPoint> awaitDataRegionLeaderInternalEndPoints(
      final Session session, final String database) {
    final AtomicReference<Set<TEndPoint>> leaderEndPoints =
        new AtomicReference<>(Collections.emptySet());
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              final Set<TEndPoint> currentLeaderEndPoints =
                  queryDataRegionLeaderInternalEndPoints(session, database);
              assertFalse(currentLeaderEndPoints.isEmpty());
              leaderEndPoints.set(currentLeaderEndPoints);
            });
    return leaderEndPoints.get();
  }

  public static Session getUnderlyingSession(final ITableSession tableSession)
      throws ReflectiveOperationException {
    Class<?> currentClass = tableSession.getClass();
    while (currentClass != null) {
      for (final java.lang.reflect.Field field : currentClass.getDeclaredFields()) {
        if (Session.class.equals(field.getType())) {
          field.setAccessible(true);
          return (Session) field.get(tableSession);
        }
      }
      currentClass = currentClass.getSuperclass();
    }
    throw new NoSuchFieldException("Session field not found in " + tableSession.getClass());
  }

  @SuppressWarnings("unchecked")
  public static <T> T getSessionField(final Session session, final String fieldName)
      throws ReflectiveOperationException {
    final java.lang.reflect.Field field = Session.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return (T) field.get(session);
  }

  private static Set<TEndPoint> queryDataRegionLeaderInternalEndPoints(
      final Session session, final String database) throws Exception {
    final Set<TEndPoint> leaderEndPoints = new HashSet<>();
    try (SessionDataSet dataSet = session.executeQueryStatement("SHOW REGIONS")) {
      final List<String> columnNames = dataSet.getColumnNames();
      final int typeIndex = columnNames.indexOf(ColumnHeaderConstant.TYPE);
      final int databaseIndex = columnNames.indexOf(ColumnHeaderConstant.DATABASE);
      final int rpcAddressIndex = columnNames.indexOf(ColumnHeaderConstant.RPC_ADDRESS);
      final int rpcPortIndex = columnNames.indexOf(ColumnHeaderConstant.RPC_PORT);
      final int roleIndex = columnNames.indexOf(ColumnHeaderConstant.ROLE);

      while (dataSet.hasNext()) {
        final RowRecord rowRecord = dataSet.next();
        if (!DATA_REGION.equals(getString(rowRecord, typeIndex))
            || !database.equals(getString(rowRecord, databaseIndex))
            || !LEADER.equalsIgnoreCase(getString(rowRecord, roleIndex))) {
          continue;
        }
        final String rpcAddress = getString(rowRecord, rpcAddressIndex);
        final int rpcPort = rowRecord.getFields().get(rpcPortIndex).getIntV();
        for (final DataNodeWrapper dataNode : EnvFactory.getEnv().getDataNodeWrapperList()) {
          if (dataNode.getIp().equals(rpcAddress) && dataNode.getPort() == rpcPort) {
            leaderEndPoints.add(getInternalEndPoint(dataNode));
            break;
          }
        }
      }
    }
    return leaderEndPoints;
  }

  private static String getString(final RowRecord rowRecord, final int index) {
    final Field field = rowRecord.getFields().get(index);
    return field == null ? null : field.getStringValue();
  }
}
