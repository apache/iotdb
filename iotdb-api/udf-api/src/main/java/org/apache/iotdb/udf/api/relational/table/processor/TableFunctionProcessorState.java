package org.apache.iotdb.udf.api.relational.table.processor;

import org.apache.iotdb.udf.api.relational.table.specification.TableParameterSpecification;

import org.apache.tsfile.block.column.Column;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * The result of processing input by {@link TableFunctionDataProcessor} or {@link
 * TableFunctionSplitProcessor}. It can optionally include a portion of output data in the form of
 * {@link List<Column>}
 *
 * <ul>
 *   <li>The returned {@link List<Column>} should consist of:
 *   <li>- proper columns produced by the table function
 *   <li>- one column of type {@code BIGINT} for each table function's input table having the
 *       pass-through property (see {@link TableParameterSpecification#isPassThroughColumns}), in
 *       order of the corresponding argument specifications.
 *   <li>Entries in these columns are the indexes of input rows (from partition start) to be
 *       attached to output, or null to indicate that a row of nulls should be attached instead of
 *       an input row. The indexes are validated to be within the portion of the partition provided
 *       to the function so far.
 * </ul>
 *
 * Note: when the input is empty, the only valid index value is null, because there are no input
 * rows that could be attached to output. In such case, for performance reasons, the validation of
 * indexes is skipped, and all pass-through columns are filled with nulls.
 */
public interface TableFunctionProcessorState {
  public static class Blocked implements TableFunctionProcessorState {
    private final CompletableFuture<Void> future;

    private Blocked(CompletableFuture<Void> future) {
      this.future = requireNonNull(future, "future is null");
    }

    public static Blocked blocked(CompletableFuture<Void> future) {
      return new Blocked(future);
    }

    public CompletableFuture<Void> getFuture() {
      return future;
    }
  }

  public static class Finished implements TableFunctionProcessorState {
    public static final Finished FINISHED = new Finished();

    private Finished() {}
  }

  public static class Processed implements TableFunctionProcessorState {
    private final List<Column> result;

    private Processed(List<Column> result) {
      this.result = result;
    }

    public static Processed produced(List<Column> result) {
      requireNonNull(result, "result is null");
      return new Processed(result);
    }

    public List<Column> getResult() {
      return result;
    }
  }
}
