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

package org.apache.iotdb.db.pipe.source.dataregion.realtime.disruptor;

/**
 * Sequence barrier for consumer coordination
 *
 * <p>This implementation is based on LMAX Disruptor (https://github.com/LMAX-Exchange/disruptor)
 * and simplified for IoTDB's Pipe module (removed Alert mechanism - IoTDB doesn't need it).
 *
 * <p>Core features preserved from LMAX Disruptor:
 *
 * <ul>
 *   <li>waitFor() logic for waiting sequences
 *   <li>Scan available buffer for out-of-order publishing
 * </ul>
 */
public class SequenceBarrier {
  private final MultiProducerSequencer sequencer;
  private final Sequence[] dependentSequences;

  public SequenceBarrier(MultiProducerSequencer sequencer, Sequence[] dependentSequences) {
    this.sequencer = sequencer;
    this.dependentSequences = dependentSequences != null ? dependentSequences : new Sequence[0];
  }

  /**
   * CORE: Wait for sequence to become available (MUST keep logic)
   *
   * @param sequence sequence to wait for
   * @return highest available sequence
   * @throws InterruptedException if interrupted
   */
  public long waitFor(long sequence) throws InterruptedException {
    // Wait for cursor
    long availableSequence;
    while ((availableSequence = sequencer.getCursor().get()) < sequence) {
      Thread.sleep(1);
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException();
      }
    }

    // Wait for dependent sequences
    if (dependentSequences.length > 0) {
      while (Sequence.getMinimumSequence(dependentSequences) < sequence) {
        Thread.sleep(1);
        if (Thread.currentThread().isInterrupted()) {
          throw new InterruptedException();
        }
      }
    }

    // CORE: Scan available buffer for highest continuously published sequence
    return sequencer.getHighestPublishedSequence(sequence, availableSequence);
  }

  public long getCursor() {
    return sequencer.getCursor().get();
  }
}
