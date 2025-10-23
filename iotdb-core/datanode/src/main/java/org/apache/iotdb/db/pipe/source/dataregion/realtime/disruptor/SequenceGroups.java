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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Utility for atomic management of sequence arrays
 *
 * <p>This implementation is based on LMAX Disruptor (https://github.com/LMAX-Exchange/disruptor)
 * and adapted for IoTDB's Pipe module.
 *
 * <p>Provides thread-safe operations for adding and removing sequences from gating sequence arrays
 * used to track consumer progress.
 */
final class SequenceGroups {

  /** Field updater for atomic array replacement */
  private static final AtomicReferenceFieldUpdater<MultiProducerSequencer, Sequence[]>
      SEQUENCE_UPDATER =
          AtomicReferenceFieldUpdater.newUpdater(
              MultiProducerSequencer.class, Sequence[].class, "gatingSequences");

  /**
   * Atomically add sequences to the gating sequence array
   *
   * <p>Uses CAS loop to ensure thread-safe addition even under concurrent modification
   *
   * @param sequencer the multi-producer sequencer
   * @param cursor the current cursor sequence
   * @param sequencesToAdd sequences to add
   */
  static void addSequences(
      final MultiProducerSequencer sequencer,
      final Sequence cursor,
      final Sequence... sequencesToAdd) {
    long cursorSequence;
    Sequence[] updatedSequences;
    Sequence[] currentSequences;

    do {
      currentSequences = sequencer.gatingSequences;
      updatedSequences = new Sequence[currentSequences.length + sequencesToAdd.length];
      System.arraycopy(currentSequences, 0, updatedSequences, 0, currentSequences.length);

      cursorSequence = cursor.get();

      int index = currentSequences.length;
      for (Sequence sequence : sequencesToAdd) {
        sequence.set(cursorSequence);
        updatedSequences[index++] = sequence;
      }
    } while (!SEQUENCE_UPDATER.compareAndSet(sequencer, currentSequences, updatedSequences));

    cursorSequence = cursor.get();
    for (Sequence sequence : sequencesToAdd) {
      sequence.set(cursorSequence);
    }
  }

  /**
   * Remove sequence from the group
   *
   * @param sequencer the sequencer
   * @param sequence sequence to remove
   * @return true if removed
   */
  static boolean removeSequence(final MultiProducerSequencer sequencer, final Sequence sequence) {
    int numToRemove;
    Sequence[] oldSequences;
    Sequence[] newSequences;

    do {
      oldSequences = sequencer.gatingSequences;
      numToRemove = countMatching(oldSequences, sequence);

      if (0 == numToRemove) {
        break;
      }

      final int oldSize = oldSequences.length;
      newSequences = new Sequence[oldSize - numToRemove];

      for (int i = 0, pos = 0; i < oldSize; i++) {
        final Sequence testSequence = oldSequences[i];
        if (sequence != testSequence) {
          newSequences[pos++] = testSequence;
        }
      }
    } while (!SEQUENCE_UPDATER.compareAndSet(sequencer, oldSequences, newSequences));

    return numToRemove != 0;
  }

  private static int countMatching(Sequence[] values, final Sequence toMatch) {
    int numToRemove = 0;
    for (Sequence value : values) {
      if (value == toMatch) {
        numToRemove++;
      }
    }
    return numToRemove;
  }
}
