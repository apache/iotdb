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

package org.apache.iotdb.db.mpp.plan;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.config.MultiLeaderConfig;
import org.apache.iotdb.consensus.multileader.client.MultiLeaderConsensusClientPool;
import org.apache.iotdb.consensus.multileader.client.SyncMultiLeaderServiceClient;
import org.apache.iotdb.consensus.multileader.thrift.TInactivatePeerReq;
import org.apache.iotdb.consensus.multileader.thrift.TInactivatePeerRes;
import org.apache.iotdb.consensus.multileader.thrift.TTriggerSnapshotLoadReq;
import org.apache.iotdb.consensus.multileader.thrift.TTriggerSnapshotLoadRes;
import org.apache.iotdb.db.client.DataNodeClientPoolFactory;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TMaintainPeerReq;

import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestRPCClient {
  private static final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      INTERNAL_SERVICE_CLIENT_MANAGER =
          new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
              .createClientManager(
                  new DataNodeClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());

  private final IClientManager<TEndPoint, SyncMultiLeaderServiceClient> syncClientManager;

  public TestRPCClient() {
    syncClientManager =
        new IClientManager.Factory<TEndPoint, SyncMultiLeaderServiceClient>()
            .createClientManager(
                new MultiLeaderConsensusClientPool.SyncMultiLeaderServiceClientPoolFactory(
                    new MultiLeaderConfig.Builder().build()));
  }

  public static void main(String args[]) {
    TestRPCClient client = new TestRPCClient();
    //    client.removeRegionPeer();
    client.addPeer();
    //    client.loadSnapshot();
  }

  private void loadSnapshot() {
    try (SyncMultiLeaderServiceClient client =
        syncClientManager.borrowClient(new TEndPoint("127.0.0.1", 40011))) {
      TTriggerSnapshotLoadRes res =
          client.triggerSnapshotLoad(
              new TTriggerSnapshotLoadReq(
                  new DataRegionId(1).convertToTConsensusGroupId(), "snapshot_1_1662370255552"));
      System.out.println(res.status);
    } catch (IOException | TException e) {
      throw new RuntimeException(e);
    }
  }

  private void testAddPeer() {
    try (SyncMultiLeaderServiceClient client =
        syncClientManager.borrowClient(new TEndPoint("127.0.0.1", 40012))) {
      TInactivatePeerRes res =
          client.inactivatePeer(
              new TInactivatePeerReq(new DataRegionId(1).convertToTConsensusGroupId()));
      System.out.println(res.status);
    } catch (IOException | TException e) {
      throw new RuntimeException(e);
    }
  }

  private void removeRegionPeer() {
    try (SyncDataNodeInternalServiceClient client =
        INTERNAL_SERVICE_CLIENT_MANAGER.borrowClient(new TEndPoint("127.0.0.1", 9003))) {
      client.removeRegionPeer(
          new TMaintainPeerReq(new DataRegionId(1).convertToTConsensusGroupId(), getLocation2(3)));
    } catch (IOException | TException e) {
      throw new RuntimeException(e);
    }
  }

  private void addPeer() {
    try (SyncDataNodeInternalServiceClient client =
        INTERNAL_SERVICE_CLIENT_MANAGER.borrowClient(new TEndPoint("127.0.0.1", 9003))) {
      client.addRegionPeer(
          new TMaintainPeerReq(new DataRegionId(1).convertToTConsensusGroupId(), getLocation2(3)));
    } catch (IOException | TException e) {
      throw new RuntimeException(e);
    }
  }

  private TDataNodeLocation getLocation3(int dataNodeId) {
    return new TDataNodeLocation(
        dataNodeId,
        new TEndPoint("127.0.0.1", 6669),
        new TEndPoint("127.0.0.1", 9005),
        new TEndPoint("127.0.0.1", 8779),
        new TEndPoint("127.0.0.1", 40012),
        new TEndPoint("127.0.0.1", 50012));
  }

  private TDataNodeLocation getLocation2(int dataNodeId) {
    return new TDataNodeLocation(
        dataNodeId,
        new TEndPoint("127.0.0.1", 6668),
        new TEndPoint("127.0.0.1", 9004),
        new TEndPoint("127.0.0.1", 8778),
        new TEndPoint("127.0.0.1", 40011),
        new TEndPoint("127.0.0.1", 50011));
  }

  private void createDataRegion() {
    try (SyncDataNodeInternalServiceClient client =
        INTERNAL_SERVICE_CLIENT_MANAGER.borrowClient(new TEndPoint("127.0.0.1", 9005))) {
      TCreateDataRegionReq req = new TCreateDataRegionReq();
      req.setStorageGroup("root.test.g_0");
      TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
      regionReplicaSet.setRegionId(new DataRegionId(1).convertToTConsensusGroupId());
      List<TDataNodeLocation> locationList = new ArrayList<>();
      locationList.add(
          new TDataNodeLocation(
              3,
              new TEndPoint("127.0.0.1", 6667),
              new TEndPoint("127.0.0.1", 9003),
              new TEndPoint("127.0.0.1", 8777),
              new TEndPoint("127.0.0.1", 40010),
              new TEndPoint("127.0.0.1", 50010)));
      locationList.add(
          new TDataNodeLocation(
              4,
              new TEndPoint("127.0.0.1", 6668),
              new TEndPoint("127.0.0.1", 9004),
              new TEndPoint("127.0.0.1", 8778),
              new TEndPoint("127.0.0.1", 40011),
              new TEndPoint("127.0.0.1", 50011)));
      locationList.add(
          new TDataNodeLocation(
              4,
              new TEndPoint("127.0.0.1", 6669),
              new TEndPoint("127.0.0.1", 9005),
              new TEndPoint("127.0.0.1", 8779),
              new TEndPoint("127.0.0.1", 40012),
              new TEndPoint("127.0.0.1", 50012)));
      regionReplicaSet.setDataNodeLocations(locationList);
      req.setRegionReplicaSet(regionReplicaSet);
      TSStatus res = client.createDataRegion(req);
      System.out.println(res.code + " " + res.message);

    } catch (IOException | TException e) {
      throw new RuntimeException(e);
    }
  }
}
