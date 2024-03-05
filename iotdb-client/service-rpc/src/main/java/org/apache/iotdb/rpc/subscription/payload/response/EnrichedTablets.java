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

package org.apache.iotdb.rpc.subscription.payload.response;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class EnrichedTablets {

  private String topicName;
  private List<Tablet> tablets = new ArrayList<>();

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(topicName, stream);
    ReadWriteIOUtils.write(tablets.size(), stream);
    for (Tablet tablet : tablets) {
      tablet.serialize(stream);
    }
  }

  public static EnrichedTablets deserialize(ByteBuffer buffer) {
    EnrichedTablets enrichedTablets = new EnrichedTablets();
    enrichedTablets.topicName = ReadWriteIOUtils.readString(buffer);
    int size = ReadWriteIOUtils.readInt(buffer);
    for (int i = 0; i < size; ++i) {
      enrichedTablets.tablets.add(Tablet.deserialize(buffer));
    }
    return enrichedTablets;
  }
}
