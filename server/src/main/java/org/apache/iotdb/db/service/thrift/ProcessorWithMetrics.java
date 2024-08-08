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

package org.apache.iotdb.db.service.thrift;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService.Iface;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService.Processor;

import org.apache.thrift.ProcessFunction;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;

import java.util.concurrent.TimeUnit;

public class ProcessorWithMetrics extends Processor {
  private Iface iface;

  public ProcessorWithMetrics(Iface iface) {
    super(iface);
    this.iface = iface;
  }

  @Override
  public void process(TProtocol in, TProtocol out) throws TException {
    TMessage msg = in.readMessageBegin();
    long startTime = System.currentTimeMillis();
    ProcessFunction fn = (ProcessFunction) getProcessMapView().get(msg.name);
    if (fn == null) {
      TProtocolUtil.skip(in, TType.STRUCT);
      in.readMessageEnd();
      TApplicationException x =
          new TApplicationException(
              TApplicationException.UNKNOWN_METHOD, "Invalid method name: '" + msg.name + "'");
      out.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
      x.write(out);
      out.writeMessageEnd();
      out.getTransport().flush();
    } else {
      fn.process(msg.seqid, in, out, iface);
    }
    long cost = System.currentTimeMillis() - startTime;
    MetricService.getInstance()
        .timer(
            cost,
            TimeUnit.MILLISECONDS,
            Metric.ENTRY.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            msg.name);
  }
}
