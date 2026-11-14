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

package org.apache.iotdb.consensus.traft.paper.core;

import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.DurableRecord;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Message;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Pins the version 5 wire and journal encodings byte for byte, so any later format change is
 * deliberate. Frames of older versions are rejected.
 */
public class TRaftCodecFormatTest {
  private static final String[] MESSAGES = {
    "0504250204080e040411020311050102030405040a05040b",
    "05060a0206080101040a06040b040c041100041102031105080406",
    "05052404020801041105040f",
    "050e1b06020a180604120203000002030501010501030000000a0602000667656e2dc3a901030100"
        + "0411020311050202041202030000010501030000000101040a06040b040c04110004110203110501"
        + "03040a00041102031105020903040c",
    "0506030204080104041403036401060c",
  };

  private static final String[] RECORDS = {
    "050000080401",
    "0503010801010404110203d00f03090807",
    "0501010801010004110203d00f03",
    "05020208010101040a04040b04110404110203d00f03090807",
    "05040808010404040b0411",
    "050604080102040a",
    "05010308010104041403036401060c",
  };

  private static byte[] bytes(String hex) {
    byte[] out = new byte[hex.length() / 2];
    for (int i = 0; i < out.length; i++) {
      out[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
    }
    return out;
  }

  @Test
  public void messagesEncodeToThePinnedBytesAndDecodeBack() throws Exception {
    List<Message> samples = TRaftCodecSamples.messages();
    assertEquals(MESSAGES.length, samples.size());
    for (int i = 0; i < MESSAGES.length; i++) {
      byte[] expected = bytes(MESSAGES[i]);
      assertArrayEquals("message " + i, expected, TRaftCodec.encodeMessage(samples.get(i)));
      assertArrayEquals(
          "message " + i + " round trip",
          expected,
          TRaftCodec.encodeMessage(TRaftCodec.decodeMessage(expected)));
    }
  }

  @Test
  public void recordsEncodeToThePinnedBytesAndDecodeBack() throws Exception {
    List<DurableRecord> samples = TRaftCodecSamples.records();
    assertEquals(RECORDS.length, samples.size());
    for (int i = 0; i < RECORDS.length; i++) {
      byte[] expected = bytes(RECORDS[i]);
      assertArrayEquals("record " + i, expected, TRaftCodec.encodeRecord(samples.get(i)));
      assertArrayEquals(
          "record " + i + " round trip",
          expected,
          TRaftCodec.encodeRecord(TRaftCodec.decodeRecord(expected)));
    }
  }

  @Test
  public void truncatedAndTrailingBytesFailClosed() {
    for (String hex : MESSAGES) {
      byte[] frame = bytes(hex);
      for (int cut : new int[] {0, 3, frame.length / 2, frame.length - 1}) {
        try {
          TRaftCodec.decodeMessage(Arrays.copyOf(frame, cut));
          fail("a truncated message must not decode");
        } catch (IOException expected) {
          // EOF or a bounds check
        }
      }
      try {
        TRaftCodec.decodeMessage(Arrays.copyOf(frame, frame.length + 1));
        fail("trailing bytes must be rejected");
      } catch (IOException expected) {
        // trailing TRaft frame bytes
      }
    }
    for (String hex : RECORDS) {
      byte[] frame = bytes(hex);
      try {
        TRaftCodec.decodeRecord(Arrays.copyOf(frame, frame.length - 1));
        fail("a truncated record must not decode");
      } catch (IOException expected) {
        // EOF or a bounds check
      }
    }
  }
}
