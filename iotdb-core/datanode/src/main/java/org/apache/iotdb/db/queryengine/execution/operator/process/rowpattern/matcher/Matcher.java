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

package org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.matcher;

import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.PatternAggregator;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.PatternAggregators;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.PatternVariableRecognizer;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.matcher.MatchResult.NO_MATCH;

public class Matcher {
  private final Program program;

  //  private final ThreadEquivalence threadEquivalence;
  private final List<PatternAggregator> patternAggregators;

  private static class Runtime {
    private static final long INSTANCE_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(Runtime.class);

    // a helper structure for identifying equivalent threads
    // program pointer (instruction) --> list of threads that have reached this instruction
    private final IntMultimap threadsAtInstructions;
    // threads that should be killed as determined by the current iteration of the main loop
    // they are killed after the iteration so that they can be used to kill other threads while the
    // iteration lasts
    private final IntList threadsToKill;

    private final IntList threads;
    private final IntStack freeThreadIds;
    private int newThreadId;
    private final int inputLength;
    private final boolean matchingAtPartitionStart;
    private final PatternCaptures patternCaptures;

    // for each thread, array of MatchAggregations evaluated by this thread
    private final PatternAggregators aggregators;

    public Runtime(
        Program program,
        int inputLength,
        boolean matchingAtPartitionStart,
        List<PatternAggregator> patternAggregators) {
      int initialCapacity = 2 * program.size();
      threads = new IntList(initialCapacity);
      freeThreadIds = new IntStack(initialCapacity);
      this.patternCaptures =
          new PatternCaptures(
              initialCapacity, program.getMinSlotCount(), program.getMinLabelCount());
      this.inputLength = inputLength;
      this.matchingAtPartitionStart = matchingAtPartitionStart;
      this.aggregators = new PatternAggregators(initialCapacity, patternAggregators);
      //      this.aggregations =
      //          new MatchAggregations(
      //              initialCapacity, aggregationInstantiators, aggregationsMemoryContext);

      this.threadsAtInstructions = new IntMultimap(program.size(), program.size());
      this.threadsToKill = new IntList(initialCapacity);
    }

    private int forkThread(int parent) {
      int child = newThread();
      patternCaptures.copy(parent, child);
      aggregators.copy(parent, child);
      return child;
    }

    private int newThread() {
      if (freeThreadIds.size() > 0) {
        return freeThreadIds.pop();
      }
      return newThreadId++;
    }

    private void scheduleKill(int threadId) {
      threadsToKill.add(threadId);
    }

    private void killThreads() {
      for (int i = 0; i < threadsToKill.size(); i++) {
        killThread(threadsToKill.get(i));
      }
      threadsToKill.clear();
    }

    private void killThread(int threadId) {
      freeThreadIds.push(threadId);
      patternCaptures.release(threadId);
      aggregators.release(threadId);
    }

    private long getSizeInBytes() {
      return INSTANCE_SIZE
          + threadsAtInstructions.getSizeInBytes()
          + threadsToKill.getSizeInBytes()
          + threads.getSizeInBytes()
          + freeThreadIds.getSizeInBytes()
          + patternCaptures.getSizeInBytes();
      //                + patternAggregators.getSizeInBytes();
    }
  }

  public Matcher(Program program, List<PatternAggregator> patternAggregators) {
    this.program = program;
    this.patternAggregators = patternAggregators;
  }

  public MatchResult run(PatternVariableRecognizer patternVariableRecognizer) {
    IntList current = new IntList(program.size());
    IntList next = new IntList(program.size());

    int inputLength = patternVariableRecognizer.getInputLength();
    boolean matchingAtPartitionStart = patternVariableRecognizer.isMatchingAtPartitionStart();

    Runtime runtime =
        new Runtime(program, inputLength, matchingAtPartitionStart, patternAggregators);

    advanceAndSchedule(current, runtime.newThread(), 0, 0, runtime);

    MatchResult result = NO_MATCH;

    for (int index = 0; index < inputLength; index++) {
      if (current.size() == 0) {
        // no match found -- all threads are dead
        break;
      }
      boolean matched = false;
      // For every existing thread, consume the label if possible. Otherwise, kill the thread.
      // After consuming the label, advance to the next `MATCH_LABEL`. Collect the advanced threads
      // in `next`,
      // which will be the starting point for the next iteration.

      // clear the structure for new input index
      runtime.threadsAtInstructions.clear();
      runtime.killThreads();

      for (int i = 0; i < current.size(); i++) {
        int threadId = current.get(i);
        int pointer = runtime.threads.get(threadId);
        Instruction instruction = program.at(pointer);
        switch (instruction.type()) {
          case MATCH_LABEL:
            int label = ((MatchLabel) instruction).getLabel();
            // save the label before evaluating the defining condition, because evaluating assumes
            // that the label is tentatively matched
            // - if the condition is true, the label is already saved
            // - if the condition is false, the thread is killed along with its patternCaptures,
            // so the
            // incorrectly saved label does not matter
            runtime.patternCaptures.saveLabel(threadId, label);
            if (patternVariableRecognizer.evaluateLabel(
                runtime.patternCaptures.getLabels(threadId), runtime.aggregators.get(threadId))) {
              advanceAndSchedule(next, threadId, pointer + 1, index + 1, runtime);
            } else {
              runtime.scheduleKill(threadId);
            }
            break;
          case DONE:
            matched = true;
            result =
                new MatchResult(
                    true,
                    runtime.patternCaptures.getLabels(threadId),
                    runtime.patternCaptures.getCaptures(threadId));
            runtime.scheduleKill(threadId);
            break;
          default:
            throw new UnsupportedOperationException("not yet implemented");
        }
        if (matched) {
          // do not process the following threads, because they are on less preferred paths than the
          // match found
          for (int j = i + 1; j < current.size(); j++) {
            runtime.scheduleKill(current.get(j));
          }
          break;
        }
      }

      IntList temp = current;
      temp.clear();
      current = next;
      next = temp;
    }

    // handle the case when the program still has instructions to process after consuming the whole
    // input
    for (int i = 0; i < current.size(); i++) {
      int threadId = current.get(i);
      if (program.at(runtime.threads.get(threadId)).type() == Instruction.Type.DONE) {
        result =
            new MatchResult(
                true,
                runtime.patternCaptures.getLabels(threadId),
                runtime.patternCaptures.getCaptures(threadId));
        break;
      }
    }

    return result;
  }

  /**
   * For a particular thread identified by `threadId`, process consecutive instructions of the
   * program, from the instruction at `pointer` up to the next instruction which consumes a label or
   * to the program end. The resulting thread state (the pointer of the first not processed
   * instruction) is recorded in `next`. There might be multiple threads recorded in `next`, as a
   * result of the instruction `SPLIT`.
   */
  private void advanceAndSchedule(
      IntList next, int threadId, int pointer, int inputIndex, Runtime runtime) {
    Instruction instruction = program.at(pointer);
    switch (instruction.type()) {
      case MATCH_START:
        if (inputIndex == 0 && runtime.matchingAtPartitionStart) {
          advanceAndSchedule(next, threadId, pointer + 1, inputIndex, runtime);
        } else {
          runtime.scheduleKill(threadId);
        }
        break;
      case MATCH_END:
        if (inputIndex == runtime.inputLength) {
          advanceAndSchedule(next, threadId, pointer + 1, inputIndex, runtime);
        } else {
          runtime.scheduleKill(threadId);
        }
        break;
      case JUMP:
        advanceAndSchedule(next, threadId, ((Jump) instruction).getTarget(), inputIndex, runtime);
        break;
      case SPLIT:
        int forked = runtime.forkThread(threadId);
        advanceAndSchedule(next, threadId, ((Split) instruction).getFirst(), inputIndex, runtime);
        advanceAndSchedule(next, forked, ((Split) instruction).getSecond(), inputIndex, runtime);
        break;
      case SAVE:
        runtime.patternCaptures.save(threadId, inputIndex);
        advanceAndSchedule(next, threadId, pointer + 1, inputIndex, runtime);
        break;
      default: // MATCH_LABEL or DONE
        runtime.threads.set(threadId, pointer);
        next.add(threadId);
        break;
    }
  }
}
