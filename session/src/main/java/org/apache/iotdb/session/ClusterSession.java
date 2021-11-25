package org.apache.iotdb.session;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ClusterSession {
  Session[] sessions;
  ArrayBlockingQueue<Tablet>[] queues;
  List<EndPoint> nodeList;

  public ClusterSession(String host, int rpcPort) throws IoTDBConnectionException {
    Session session = new Session(host, rpcPort);
    session.open();
    nodeList = new ArrayList<>();
    nodeList.addAll(session.getNodeList());
    sessions = new Session[nodeList.size()];
    queues = new ArrayBlockingQueue[nodeList.size()];
    for (int i = 0; i < nodeList.size(); i++) {
      sessions[i] = new Session(nodeList.get(i).ip, nodeList.get(i).port);
      sessions[i].open();
      queues[i] = new ArrayBlockingQueue<Tablet>(1000);
      new Thread(new RunnableTask(i)).start();
    }
  }

  public void insertTablet(Tablet tablet)
      throws StatementExecutionException, IoTDBConnectionException {
    int hashVal = tablet.prefixPath.hashCode();
    int index = hashVal % nodeList.size();
    for (int i = 0; i < 2; i++) {
      int j = (index + i) % nodeList.size();
      synchronized (queues[j]) {
        if (!queues[j].isEmpty()) {
          queues[j].add(tablet);
          queues[j].notifyAll();
          continue;
        }
      }
      try {
        sessions[j].insertTablet(tablet);
      } catch (Exception e) {
        synchronized (queues[j]) {
          queues[j].add(tablet);
          queues[j].notifyAll();
        }
      }
    }
  }

  public SessionDataSet queryTablet(String sql, String deviceId) {
    int hashVal = deviceId.hashCode();
    int index = hashVal % nodeList.size();
    SessionDataSet sessionDataSet = null;
    try {
      sessionDataSet = sessions[index].executeQueryStatement(sql);
    } catch (Exception e) {
      try {
        sessionDataSet = sessions[(index + 1) % nodeList.size()].executeQueryStatement(sql);
      } catch (Exception ex) {
        // never happen, once the node restart, it won't be killed anymore.
        e.printStackTrace();
      }
    }
    return sessionDataSet;
  }

  public Session reconnect(int index) throws IoTDBConnectionException {
    sessions[index] = new Session(nodeList.get(index).ip, nodeList.get(index).port);
    sessions[index].open();
    return sessions[index];
  }

  class RunnableTask implements Runnable {
    int index;

    public RunnableTask(int index) {
      this.index = index;
    }

    @Override
    public void run() {
        Tablet tablet;
        while (true) {
          Tablet t;
          synchronized (queues[index]) {
            if (queues[index].isEmpty()) {
              try {
                queues[index].wait(1000);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            } else {
              try {
                Session session = reconnect(index);
                t = queues[index].poll();
                session.insertTablet(t);
              } catch (StatementExecutionException | IoTDBConnectionException e) {
              }
            }
          }

        }

    }
  }
}
