package org.apache.iotdb.confignode.procedure.state;

public enum DataPartitionTableIntegrityCheckProcedureState {
  /** Collect earliest timeslot information from all DataNodes */
  COLLECT_EARLIEST_TIMESLOTS,
  /** Analyze missing data partitions */
  ANALYZE_MISSING_PARTITIONS,
  /** Request DataPartitionTable generation from DataNodes */
  REQUEST_PARTITION_TABLES,
  /** Merge DataPartitionTables from all DataNodes */
  MERGE_PARTITION_TABLES,
  /** Write final DataPartitionTable to raft log */
  WRITE_PARTITION_TABLE_TO_RAFT,
  /** Procedure completed successfully */
  SUCCESS,
  /** Procedure failed */
  FAILED
}
