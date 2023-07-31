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

package org.apache.iotdb.db.pipe.connector.v1.request;

import org.apache.iotdb.db.pipe.connector.IoTDBThriftConnectorRequestVersion;
import org.apache.iotdb.db.pipe.connector.v1.PipeRequestType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class PipeTransferBatchReq extends TPipeTransferReq {

  private InsertNode[] insertNodes = new InsertNode[1024];
  private Tablet[] tablets;
  private boolean[] isAlignedArray;

  private PipeTransferBatchReq() {
    // Do nothing
  }

  public static PipeTransferBatchReq toTPipeTransferBatchReq(List<TPipeTransferReq> reqs)
      throws IOException {
    final PipeTransferBatchReq req = new PipeTransferBatchReq();

    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    ReadWriteIOUtils.write(reqs.size(), stream);

    for (TPipeTransferReq tReq : reqs) {

      ReadWriteIOUtils.write(tReq.type, stream);
      stream.write(tReq.body.array());
      //      ReadWriteIOUtils.write(tReq.body, stream);
    }

    req.version = IoTDBThriftConnectorRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_BATCH.getType();
    req.body = ByteBuffer.wrap(stream.toByteArray());

    return req;
  }
}
