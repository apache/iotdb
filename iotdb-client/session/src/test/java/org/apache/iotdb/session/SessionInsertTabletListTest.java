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

package org.apache.iotdb.session;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RedirectException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletsReq;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** {@link Session#insertTablets(List)} and {@link Session#insertAlignedTablets(List)}. */
public class SessionInsertTabletListTest {

  private static final TEndPoint DEFAULT_ENDPOINT = new TEndPoint("127.0.0.1", 55560);
  private static final TEndPoint LEADER_OF_D1 = new TEndPoint("127.0.0.1", 55561);
  private static final TEndPoint LEADER_OF_D2 = new TEndPoint("127.0.0.1", 55562);

  private static final String D1 = "root.db.d1";
  private static final String D2 = "root.db.d2";

  private static final Map<String, TEndPoint> LEADERS = new HashMap<>();

  static {
    LEADERS.put(D1, LEADER_OF_D1);
    LEADERS.put(D2, LEADER_OF_D2);
  }

  /** The requests each endpoint received, in order. */
  private final Map<TEndPoint, List<TSInsertTabletsReq>> requests = new LinkedHashMap<>();

  private Session session;

  @After
  public void tearDown() throws IoTDBConnectionException {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void test_InsertTablets_WhenLeadersAreUnknown_SendsAllTabletsToDefaultEndpoint()
      throws IoTDBConnectionException, StatementExecutionException {
    session = openSession(true);

    session.insertTablets(Arrays.asList(tablet(D1, "s1"), tablet(D1, "s2"), tablet(D2, "s1")));

    assertEquals(Collections.singleton(DEFAULT_ENDPOINT), requests.keySet());
    assertEquals(1, requests.get(DEFAULT_ENDPOINT).size());
    assertEquals(Arrays.asList(D1, D1, D2), requests.get(DEFAULT_ENDPOINT).get(0).getPrefixPaths());
    // the redirect taught the session the leaders
    assertEquals(LEADER_OF_D1, session.deviceIdToEndpoint.get(D1));
    assertEquals(LEADER_OF_D2, session.deviceIdToEndpoint.get(D2));
  }

  @Test
  public void test_InsertTablets_WhenLeadersAreKnown_SendsOneRequestPerLeader()
      throws IoTDBConnectionException, StatementExecutionException {
    session = openSession(true);
    session.insertTablets(Arrays.asList(tablet(D1, "s1"), tablet(D2, "s1")));
    requests.clear();

    session.insertTablets(
        Arrays.asList(tablet(D1, "s1"), tablet(D2, "s1"), tablet(D1, "s2"), tablet(D1, "s3")));

    assertEquals(2, requests.size());
    assertEquals(1, requests.get(LEADER_OF_D1).size());
    TSInsertTabletsReq toLeaderOfD1 = requests.get(LEADER_OF_D1).get(0);
    assertEquals(Arrays.asList(D1, D1, D1), toLeaderOfD1.getPrefixPaths());
    assertEquals(
        Arrays.asList(
            Collections.singletonList("s1"),
            Collections.singletonList("s2"),
            Collections.singletonList("s3")),
        toLeaderOfD1.getMeasurementsList());
    assertEquals(1, requests.get(LEADER_OF_D2).size());
    assertEquals(Collections.singletonList(D2), requests.get(LEADER_OF_D2).get(0).getPrefixPaths());
  }

  @Test
  public void test_InsertTablets_WhenRedirectionIsDisabled_SendsOneRequestToDefaultEndpoint()
      throws IoTDBConnectionException, StatementExecutionException {
    session = openSession(false);

    session.insertTablets(Arrays.asList(tablet(D1, "s1"), tablet(D1, "s2"), tablet(D2, "s1")));

    assertEquals(Collections.singleton(DEFAULT_ENDPOINT), requests.keySet());
    assertEquals(1, requests.get(DEFAULT_ENDPOINT).size());
    TSInsertTabletsReq request = requests.get(DEFAULT_ENDPOINT).get(0);
    assertEquals(Arrays.asList(D1, D1, D2), request.getPrefixPaths());
    assertFalse(request.isIsAligned());
    assertNull(session.deviceIdToEndpoint);
  }

  @Test
  public void test_InsertAlignedTablets_WhenLeadersAreKnown_SendsAlignedRequestPerLeader()
      throws IoTDBConnectionException, StatementExecutionException {
    session = openSession(true);
    session.insertAlignedTablets(Arrays.asList(tablet(D1, "s1"), tablet(D2, "s1")));
    requests.clear();

    session.insertAlignedTablets(Arrays.asList(tablet(D1, "s1"), tablet(D1, "s2")));

    assertEquals(Collections.singleton(LEADER_OF_D1), requests.keySet());
    TSInsertTabletsReq request = requests.get(LEADER_OF_D1).get(0);
    assertEquals(Arrays.asList(D1, D1), request.getPrefixPaths());
    assertTrue(request.isIsAligned());
  }

  @Test
  public void test_InsertAlignedTablets_WhenRedirectionIsDisabled_SendsOneAlignedRequest()
      throws IoTDBConnectionException, StatementExecutionException {
    session = openSession(false);

    session.insertAlignedTablets(Arrays.asList(tablet(D1, "s1"), tablet(D1, "s2")));

    assertEquals(Collections.singleton(DEFAULT_ENDPOINT), requests.keySet());
    TSInsertTabletsReq request = requests.get(DEFAULT_ENDPOINT).get(0);
    assertEquals(Arrays.asList(D1, D1), request.getPrefixPaths());
    assertTrue(request.isIsAligned());
  }

  private Session openSession(boolean enableRedirection) throws IoTDBConnectionException {
    Session newSession = new RecordingSession(enableRedirection);
    newSession.open();
    return newSession;
  }

  /** A tablet of one INT64 measurement with two rows. */
  private static Tablet tablet(String deviceId, String measurement) {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema(measurement, TSDataType.INT64));
    Tablet tablet = new Tablet(deviceId, schemas, 2);
    for (int row = 0; row < 2; row++) {
      tablet.addTimestamp(row, row);
      tablet.addValue(measurement, row, (long) row);
    }
    return tablet;
  }

  private class RecordingSession extends Session {

    RecordingSession(boolean enableRedirection) {
      super(
          DEFAULT_ENDPOINT.getIp(),
          DEFAULT_ENDPOINT.getPort(),
          SessionConfig.DEFAULT_USER,
          SessionConfig.DEFAULT_PASSWORD,
          SessionConfig.DEFAULT_FETCH_SIZE,
          null,
          SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
          SessionConfig.DEFAULT_MAX_FRAME_SIZE,
          enableRedirection,
          SessionConfig.DEFAULT_VERSION);
      this.enableAutoFetch = false;
    }

    @Override
    public SessionConnection constructSessionConnection(
        Session session, TEndPoint endpoint, ZoneId zoneId) {
      return new RecordingSessionConnection(session, endpoint);
    }
  }

  /**
   * Records every request and, like a DataNode, answers with a redirect if the request contains
   * devices whose leader is another endpoint.
   */
  private class RecordingSessionConnection extends SessionConnection {

    private final TEndPoint endPoint;

    RecordingSessionConnection(Session session, TEndPoint endPoint) {
      super(session.sqlDialect);
      this.endPoint = endPoint;
    }

    @Override
    public void close() {}

    @Override
    protected void insertTablets(TSInsertTabletsReq request) throws RedirectException {
      requests.computeIfAbsent(endPoint, k -> new ArrayList<>()).add(request);
      Map<String, TEndPoint> redirects = new HashMap<>();
      for (String deviceId : request.getPrefixPaths()) {
        if (!LEADERS.get(deviceId).equals(endPoint)) {
          redirects.put(deviceId, LEADERS.get(deviceId));
        }
      }
      if (!redirects.isEmpty()) {
        throw new RedirectException(redirects);
      }
    }
  }
}
