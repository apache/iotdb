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

package org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.PatternVariableRecognizer.PatternVariableComputation;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression.PatternExpressionComputation;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.matcher.ArrayView;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.matcher.MatchResult;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.matcher.Matcher;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.Partition;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.ColumnList;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.RowComparator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.RowsPerMatch;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SkipToPosition;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;

public final class PatternPartitionExecutor {
  private final int partitionStart;
  private final int partitionEnd;
  private final Partition partition;

  private final List<ColumnList> sortedColumns;
  private final RowComparator peerGroupComparator;
  private int peerGroupStart;
  private int peerGroupEnd;

  private final List<Integer> outputChannels;

  private int currentGroupIndex = -1;
  private int currentPosition;

  // Row Pattern Recognition
  private final RowsPerMatch rowsPerMatch;
  private final SkipToPosition skipToPosition;
  private final Optional<LogicalIndexNavigation> skipToNavigation;
  private final Matcher matcher;
  private final List<PatternVariableComputation> patternVariableComputations;
  // an array of all MatchAggregations from all row pattern measures,
  // used to reset the MatchAggregations for every new match.
  // each of MeasureComputations also has access to the MatchAggregations,
  // and uses them to compute the result values
  private final PatternAggregator[] patternAggregators;
  private final List<PatternExpressionComputation> measureComputations;
  private final List<String> labelNames;

  private int lastSkippedPosition;
  private int lastMatchedPosition;
  private long matchNumber;

  public PatternPartitionExecutor(
      List<TsBlock> tsBlocks,
      List<TSDataType> dataTypes,
      int startIndexInFirstBlock,
      int endIndexInLastBlock,
      List<Integer> outputChannels,
      List<Integer> sortChannels,
      RowsPerMatch rowsPerMatch,
      SkipToPosition skipToPosition,
      Optional<LogicalIndexNavigation> skipToNavigation,
      Matcher matcher,
      List<PatternVariableComputation> patternVariableComputations,
      List<PatternAggregator> patternAggregators,
      List<PatternExpressionComputation> measureComputations,
      List<String> labelNames) {
    // Partition
    this.partition = new Partition(tsBlocks, startIndexInFirstBlock, endIndexInLastBlock);
    this.partitionStart = startIndexInFirstBlock;
    this.partitionEnd = startIndexInFirstBlock + this.partition.getPositionCount();

    // Output
    this.outputChannels = ImmutableList.copyOf(outputChannels);

    // Row Pattern Recognition
    this.rowsPerMatch = rowsPerMatch;
    this.skipToPosition = skipToPosition;
    this.skipToNavigation = skipToNavigation;
    this.matcher = matcher;
    this.patternVariableComputations = ImmutableList.copyOf(patternVariableComputations);
    this.patternAggregators = patternAggregators.toArray(new PatternAggregator[] {});
    this.measureComputations = ImmutableList.copyOf(measureComputations);
    this.labelNames = ImmutableList.copyOf(labelNames);

    this.lastSkippedPosition = partitionStart - 1;
    this.lastMatchedPosition = partitionStart - 1;
    this.matchNumber = 1;

    // Prepare for peer group comparing
    List<TSDataType> sortDataTypes = new ArrayList<>();
    for (int channel : sortChannels) {
      TSDataType dataType = dataTypes.get(channel);
      sortDataTypes.add(dataType);
    }
    peerGroupComparator = new RowComparator(sortDataTypes);
    sortedColumns = partition.getSortedColumnList(sortChannels);

    currentPosition = partitionStart;
    updatePeerGroup();
  }

  public boolean hasNext() {
    return currentPosition < partitionEnd;
  }

  public void processNextRow(TsBlockBuilder builder) {
    if (currentPosition == peerGroupEnd) {
      updatePeerGroup();
    }

    if (!isSkipped(currentPosition)) {
      // try match pattern from the current row on
      // 1. determine pattern search boundaries. for MATCH_RECOGNIZE, pattern matching and
      // associated computations can involve the whole partition
      int searchStart = partitionStart;
      int searchEnd = partitionEnd;
      int patternStart = currentPosition;

      // 2. match pattern variables according to the DEFINE clause
      PatternVariableRecognizer patternVariableRecognizer =
          new PatternVariableRecognizer(
              matchNumber,
              patternStart,
              partitionStart,
              searchStart,
              searchEnd,
              patternVariableComputations,
              partition);
      MatchResult matchResult = matcher.run(patternVariableRecognizer);

      // 3. produce output depending on match and output mode (rowsPerMatch)
      if (!matchResult.isMatched()) { // unmatched
        // the match from the current row was not successfully matched and the current row is not in
        // the already successfully matched rows
        if (rowsPerMatch.isUnmatchedRows() && !isMatched(currentPosition)) {
          outputUnmatchedRow(builder);
        }
        lastSkippedPosition = currentPosition;
      } else if (matchResult.getLabels().length() == 0) { // empty match
        if (rowsPerMatch.isEmptyMatches()) {
          outputEmptyMatch(builder);
        }
        lastSkippedPosition = currentPosition;
        matchNumber++;
      } else { // non-empty match
        for (PatternAggregator patternAggregator : patternAggregators) {
          patternAggregator.reset();
        }

        if (rowsPerMatch.isOneRow()) {
          outputOneRowPerMatch(builder, matchResult, patternStart, searchStart, searchEnd);
        } else {
          outputAllRowsPerMatch(builder, matchResult, searchStart, searchEnd);
        }
        // update lastMatchedPosition
        updateLastMatchedPosition(matchResult, patternStart);
        // update lastSkippedPosition
        skipAfterMatch(matchResult, patternStart, searchStart, searchEnd);
        matchNumber++;
      }
    }

    currentPosition++;
  }

  private boolean isSkipped(int position) {
    return position <= lastSkippedPosition;
  }

  private boolean isMatched(int position) {
    return position <= lastMatchedPosition;
  }

  // for ALL ROWS PER MATCH WITH UNMATCHED ROWS
  // the output for unmatched row refers to no pattern match.
  private void outputUnmatchedRow(TsBlockBuilder builder) {
    // Copy origin data
    int index = currentPosition - partitionStart;
    Partition.PartitionIndex partitionIndex = partition.getPartitionIndex(index);
    int tsBlockIndex = partitionIndex.getTsBlockIndex();
    int offsetInTsBlock = partitionIndex.getOffsetInTsBlock();
    TsBlock tsBlock = partition.getTsBlock(tsBlockIndex);

    int channel = 0;
    for (int i = 0; i < outputChannels.size(); i++) {
      Column column = tsBlock.getColumn(outputChannels.get(i));
      ColumnBuilder columnBuilder = builder.getColumnBuilder(i);
      columnBuilder.write(column, offsetInTsBlock);
      channel++;
    }

    // measures are all null for no match
    for (int i = 0; i < measureComputations.size(); i++) {
      builder.getColumnBuilder(channel).appendNull();
      channel++;
    }

    builder.declarePosition();
  }

  // the output for empty match refers to empty pattern match.
  private void outputEmptyMatch(TsBlockBuilder builder) {
    // Copy origin data
    int index = currentPosition - partitionStart;
    Partition.PartitionIndex partitionIndex = partition.getPartitionIndex(index);
    int tsBlockIndex = partitionIndex.getTsBlockIndex();
    int offsetInTsBlock = partitionIndex.getOffsetInTsBlock();
    TsBlock tsBlock = partition.getTsBlock(tsBlockIndex);

    int channel = 0;
    for (int i = 0; i < outputChannels.size(); i++) {
      Column column = tsBlock.getColumn(outputChannels.get(i));
      ColumnBuilder columnBuilder = builder.getColumnBuilder(i);
      columnBuilder.write(column, offsetInTsBlock);
      channel++;
    }

    // compute measures
    for (PatternExpressionComputation measureComputation : measureComputations) {
      Object result = measureComputation.computeEmpty(matchNumber);
      if (result == null) {
        builder.getColumnBuilder(channel).appendNull();
      } else {
        builder.getColumnBuilder(channel).writeObject(result);
      }

      channel++;
    }

    builder.declarePosition();
  }

  // output one row for the entire match
  private void outputOneRowPerMatch(
      TsBlockBuilder builder,
      MatchResult matchResult,
      int patternStart,
      int searchStart,
      int searchEnd) {
    // Copy origin data
    int index = currentPosition - partitionStart;
    Partition.PartitionIndex partitionIndex = partition.getPartitionIndex(index);
    int tsBlockIndex = partitionIndex.getTsBlockIndex();
    int offsetInTsBlock = partitionIndex.getOffsetInTsBlock();
    TsBlock tsBlock = partition.getTsBlock(tsBlockIndex);

    int channel = 0;
    // PARTITION BY
    for (int i = 0; i < outputChannels.size(); i++) {
      Column column = tsBlock.getColumn(outputChannels.get(i));
      ColumnBuilder columnBuilder = builder.getColumnBuilder(i);
      columnBuilder.write(column, offsetInTsBlock);

      channel++;
    }

    // MEASURES
    // compute measures from the position of the last row of the match
    ArrayView labels = matchResult.getLabels();
    for (PatternExpressionComputation measureComputation : measureComputations) {
      Object result =
          measureComputation.compute(
              // evaluate the MEASURES clause with the last row in the match
              patternStart + labels.length() - 1,
              labels,
              patternAggregators,
              partitionStart,
              searchStart,
              searchEnd,
              patternStart,
              matchNumber,
              labelNames,
              partition);

      if (result == null) {
        builder.getColumnBuilder(channel).appendNull();
      } else if (result instanceof String) { // special handling for CLASSIFIER()
        String str = (String) result;
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8); // specified character encoding
        Binary binary = new Binary(bytes);
        builder.getColumnBuilder(channel).writeBinary(binary);
      } else {
        builder.getColumnBuilder(channel).writeObject(result);
      }

      channel++;
    }

    builder.declarePosition();
  }

  // output each row in the match, except for rows captured by exclusion
  private void outputAllRowsPerMatch(
      TsBlockBuilder builder, MatchResult matchResult, int searchStart, int searchEnd) {
    // the final matching result
    ArrayView labels = matchResult.getLabels();
    // provide the line ranges of pattern variables to be skipped in output
    ArrayView exclusions = matchResult.getExclusions();

    int start = 0;
    for (int index = 0; index < exclusions.length(); index += 2) {
      // [exclusions[index], exclusions[index + 1]) is an exclusion range
      int end = exclusions.get(index);

      for (int i = start; i < end; i++) {
        outputRow(builder, labels, currentPosition + i, searchStart, searchEnd);
      }

      start = exclusions.get(index + 1);
    }

    for (int i = start; i < labels.length(); i++) {
      outputRow(builder, labels, currentPosition + i, searchStart, searchEnd);
    }
  }

  // Called by method outputAllRowsPerMatch
  private void outputRow(
      TsBlockBuilder builder, ArrayView labels, int position, int searchStart, int searchEnd) {
    // Copy origin data
    Partition.PartitionIndex partitionIndex = partition.getPartitionIndex(position);
    int tsBlockIndex = partitionIndex.getTsBlockIndex();
    int offsetInTsBlock = partitionIndex.getOffsetInTsBlock();
    TsBlock tsBlock = partition.getTsBlock(tsBlockIndex);

    // map the column data of the current row from the input table to the output table
    int channel = 0;
    for (int i = 0; i < outputChannels.size(); i++) {
      Column column = tsBlock.getColumn(outputChannels.get(i));
      ColumnBuilder columnBuilder = builder.getColumnBuilder(i);
      columnBuilder.write(column, offsetInTsBlock);
      channel++;
    }

    // compute measures from the current position (the position from which measures are computed
    // matters in RUNNING semantics)
    for (PatternExpressionComputation measureComputation : measureComputations) {
      Object result =
          measureComputation.compute(
              position,
              labels,
              patternAggregators,
              partitionStart,
              searchStart,
              searchEnd,
              currentPosition,
              matchNumber,
              labelNames,
              partition);

      if (result == null) {
        builder.getColumnBuilder(channel).appendNull();
      } else if (result instanceof String) { // special handling for CLASSIFIER()
        String str = (String) result;
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8); // 指定字符编码
        Binary binary = new Binary(bytes);
        builder.getColumnBuilder(channel).writeBinary(binary);
      } else {
        builder.getColumnBuilder(channel).writeObject(result);
      }

      channel++;
    }

    builder.declarePosition();
  }

  private void updateLastMatchedPosition(MatchResult matchResult, int patternStart) {
    int lastPositionInMatch = patternStart + matchResult.getLabels().length() - 1;
    lastMatchedPosition = max(lastMatchedPosition, lastPositionInMatch);
  }

  private void skipAfterMatch(
      MatchResult matchResult, int patternStart, int searchStart, int searchEnd) {
    ArrayView labels = matchResult.getLabels();
    switch (skipToPosition) {
      case PAST_LAST: // start the next search from the next row of the previous match
        lastSkippedPosition = patternStart + labels.length() - 1;
        break;
      case NEXT: // start the next search from the next row of the current row
        lastSkippedPosition = currentPosition;
        break;
      case LAST:
      case FIRST:
        checkState(
            skipToNavigation.isPresent(),
            "skip to navigation is missing for SKIP TO %s",
            skipToPosition.name());
        int position =
            skipToNavigation
                .get()
                .resolvePosition(
                    patternStart + labels.length() - 1,
                    labels,
                    searchStart,
                    searchEnd,
                    patternStart);
        if (position == -1) {
          throw new SemanticException(
              "AFTER MATCH SKIP TO failed: pattern variable is not present in match");
        }
        if (position == patternStart) {
          throw new SemanticException(
              "AFTER MATCH SKIP TO failed: cannot skip to first row of match");
        }
        lastSkippedPosition = position - 1;
        break;
      default:
        throw new IllegalStateException("unexpected SKIP TO position: " + skipToPosition);
    }
  }

  private void updatePeerGroup() {
    currentGroupIndex++;
    peerGroupStart = currentPosition;
    // Find end of peer group
    peerGroupEnd = peerGroupStart + 1;
    while (peerGroupEnd < partitionEnd
        && peerGroupComparator.equalColumnLists(
            sortedColumns, peerGroupStart - partitionStart, peerGroupEnd - partitionStart)) {
      peerGroupEnd++;
    }
  }
}
