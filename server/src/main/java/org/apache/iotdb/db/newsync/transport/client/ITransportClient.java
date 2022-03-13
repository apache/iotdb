package org.apache.iotdb.db.newsync.transport.client;

import org.apache.iotdb.service.transport.thrift.SyncRequest;
import org.apache.iotdb.service.transport.thrift.SyncResponse;

import org.apache.thrift.TException;

public interface ITransportClient extends Runnable {
  SyncResponse heartbeat(SyncRequest syncRequest) throws TException;
}
