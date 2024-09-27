/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.expr.executor;

import org.apache.iotdb.db.expr.entity.SimDeletion;
import org.apache.iotdb.db.expr.entity.SimModFile;
import org.apache.iotdb.db.expr.entity.SimTsFile;
import org.apache.iotdb.db.expr.event.Event;
import org.apache.iotdb.db.expr.event.ExecuteRangeQueryEvent;
import org.apache.iotdb.db.expr.event.GenerateDeletionEvent;
import org.apache.iotdb.db.expr.event.GenerateTsFileEvent;
import org.apache.iotdb.db.expr.simulator.SimpleSimulator.SimpleContext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SimpleExecutor implements EventExecutor<SimpleContext> {

  @Override
  public void execute(Event event, SimpleContext context) {
    if (event instanceof GenerateTsFileEvent) {
      doExecute(((GenerateTsFileEvent) event), context);
    } else if (event instanceof GenerateDeletionEvent) {
      doExecute(((GenerateDeletionEvent) event), context);
    } else if (event instanceof ExecuteRangeQueryEvent) {
      doExecute(((ExecuteRangeQueryEvent) event), context);
    }

    event.executionStep = context.getSimulator().getCurrentStep();
    event.executionTimestamp = context.getSimulator().getCurrentTimestamp();
  }

  private void doExecute(GenerateTsFileEvent event, SimpleContext simpleContext) {
    simpleContext.tsFileManager.tsFileList.add(event.currentTSFile);

    simpleContext.getStatistics().tsFileGeneratedCnt++;
  }

  private void doExecute(GenerateDeletionEvent event, SimpleContext simpleContext) {
    Set<SimModFile> involvedModFiles = new HashSet<>();
    for (SimTsFile simTsFile : simpleContext.tsFileManager.tsFileList) {
      if (simTsFile.shouldDelete(event.currentDeletion)) {
        involvedModFiles.add(simTsFile.getModFileMayAllocate());
      }
    }

    for (SimModFile involvedModFile : involvedModFiles) {
      involvedModFile.add(event.currentDeletion);
    }
    event.involvedModFiles = involvedModFiles;

    simpleContext.getStatistics().totalDeletionWriteBytes +=
        involvedModFiles.size() * simpleContext.getConfig().deletionSizeInByte;
    if (event.currentDeletion.timeRange.getMax() != Long.MAX_VALUE) {
      simpleContext.getStatistics().partialDeletionExecutedCnt++;
      simpleContext.getStatistics().partialDeletionExecutedTime += event.getTimeConsumption();
    } else {
      simpleContext.getStatistics().fullDeletionExecutedCnt++;
      simpleContext.getStatistics().fullDeletionExecutedTime += event.getTimeConsumption();
    }
  }

  private void doExecute(ExecuteRangeQueryEvent event, SimpleContext simpleContext) {
    List<SimTsFile> queriedTsFiles = new ArrayList<>();
    List<List<SimDeletion>> queriedDeletions = new ArrayList<>();
    for (SimTsFile simTsFile : simpleContext.tsFileManager.tsFileList) {
      if (simTsFile.timeRange.overlaps(event.timeRange)) {
        queriedTsFiles.add(simTsFile);
        List<SimDeletion> deletions = simTsFile.getDeletions();
        queriedDeletions.add(deletions);
        if (!deletions.isEmpty()) {
          simpleContext.getStatistics().totalDeletionReadBytes +=
              deletions.size() * simpleContext.getConfig().deletionSizeInByte;
          simpleContext.getStatistics().queryReadDeletionCnt += deletions.size();
          simpleContext.getStatistics().queryReadModsCnt += 1;
        }
      }
    }

    event.queriedTsFiles = queriedTsFiles;
    event.queriedDeletions = queriedDeletions;

    simpleContext.getStatistics().queryExecutedCnt++;
    simpleContext.getStatistics().queryExecutedTime += event.getTimeConsumption();
    simpleContext.getStatistics().queryReadDeletionTime += Math.round(event.readDeletionTimeSum);
    simpleContext.getStatistics().queryReadDeletionSeekTime +=
        Math.round(event.readDeletionSeekTimeSum);
    simpleContext.getStatistics().queryReadDeletionTransTime +=
        Math.round(event.readDeletionTransTimeSum);
    //    System.out.println(
    //        simpleContext.getSimulator().currentStep + " " +
    // simpleContext.getSimulator().currentTimestamp
    //            + " " + event.readDeletionSeekTimeSum + " " + event.readDeletionTransTimeSum);
  }
}
