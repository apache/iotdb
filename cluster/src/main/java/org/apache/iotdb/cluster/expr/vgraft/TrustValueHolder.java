package org.apache.iotdb.cluster.expr.vgraft;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.utils.ClientUtils;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TrustValueHolder {

  private static final Logger logger = LoggerFactory.getLogger(TrustValueHolder.class);

  public static final long DEFAULT_TRUST_VALUE = 50;
  private final Map<Node, Long> trustValueMap = new ConcurrentHashMap<>();
  private final RaftMember member;

  public TrustValueHolder(RaftMember member) {
    this.member = member;
  }

  public void init(PartitionGroup partitionGroup) {
    for (Node node : partitionGroup) {
      trustValueMap.put(node, DEFAULT_TRUST_VALUE);
    }
  }

  public static int verifierGroupSize(int N) {
    return ((N - 1) / 2) % 2 == 0 ? ((N - 1) / 2) + 1 : ((N - 1) / 2) + 2;
  }

  public List<Node> chooseVerifiers() {
    List<Map.Entry<Node, Long>> nodeTrustValueList = new ArrayList<>(trustValueMap.entrySet());
    nodeTrustValueList.sort(Map.Entry.comparingByValue());

    int N = nodeTrustValueList.size();
    int M = verifierGroupSize(N);
    int lowTrustThreshold = (int) Math.floor(N * 0.8);
    if (lowTrustThreshold < M) {
      lowTrustThreshold = M;
    }
    Collections.reverse(nodeTrustValueList);
    nodeTrustValueList = nodeTrustValueList.subList(0, lowTrustThreshold);

    return pingVerifiers(nodeTrustValueList, M);
  }

  private List<Node> pingVerifiers(List<Map.Entry<Node, Long>> nodeTrustValueList, int M) {
    CountDownLatch countDownLatch = new CountDownLatch(M);
    List<Node> verifiers = new ArrayList<>();

    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      pingVerifiersAsync(nodeTrustValueList, M, verifiers, countDownLatch);
    } else {
      pingVerifiersSync(nodeTrustValueList, M, verifiers, countDownLatch);
    }

    try {
      if (countDownLatch.await(
          ClusterDescriptor.getInstance().getConfig().getConnectionTimeoutInMS(),
          TimeUnit.MILLISECONDS)) {
        return verifiers;
      } else {
        return nodeTrustValueList.subList(0, M).stream()
            .map(Entry::getKey)
            .collect(Collectors.toList());
      }
    } catch (InterruptedException e) {
      return nodeTrustValueList.subList(0, M).stream()
          .map(Entry::getKey)
          .collect(Collectors.toList());
    }
  }

  private AsyncMethodCallback<Void> getPingCallback(
      Entry<Node, Long> entry, int M, List<Node> verifiers, CountDownLatch countDownLatch) {
    return new AsyncMethodCallback<Void>() {
      @Override
      public void onComplete(Void unused) {
        synchronized (verifiers) {
          if (verifiers.size() < M) {
            verifiers.add(entry.getKey());
          }
        }
        countDownLatch.countDown();
      }

      @Override
      public void onError(Exception e) {
        logger.warn("Cannot ping {} : ", entry.getKey(), e);
      }
    };
  }

  private void pingVerifiersSync(
      List<Map.Entry<Node, Long>> nodeTrustValueList,
      int M,
      List<Node> verifiers,
      CountDownLatch countDownLatch) {

    for (Map.Entry<Node, Long> entry : nodeTrustValueList) {
      AsyncMethodCallback<Void> callback = getPingCallback(entry, M, verifiers, countDownLatch);

      member
          .getSerialToParallelPool()
          .submit(
              () -> {
                Client syncClient = member.getSyncClient(entry.getKey());
                try {
                  syncClient.ping();
                  callback.onComplete(null);
                } catch (TException e) {
                  callback.onError(e);
                } finally {
                  ClientUtils.putBackSyncClient(syncClient);
                }
              });
    }
  }

  private void pingVerifiersAsync(
      List<Map.Entry<Node, Long>> nodeTrustValueList,
      int M,
      List<Node> verifiers,
      CountDownLatch countDownLatch) {

    for (Map.Entry<Node, Long> entry : nodeTrustValueList) {
      AsyncMethodCallback<Void> callback = getPingCallback(entry, M, verifiers, countDownLatch);

      RaftService.AsyncClient asyncClient = member.getAsyncClient(entry.getKey());
      try {
        asyncClient.ping(callback);
      } catch (TException e) {
        logger.warn("Cannot ping {} : ", entry.getKey(), e);
      }
    }
  }
}
