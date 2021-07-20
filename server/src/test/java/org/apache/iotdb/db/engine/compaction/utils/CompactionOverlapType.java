package org.apache.iotdb.db.engine.compaction.utils;

public enum CompactionOverlapType {
  FILE_NO_OVERLAP,
  FILE_OVERLAP_CHUNK_NO_OVERLAP,
  FILE_OVERLAP_CHUNK_OVERLAP_PAGE_NO_OVERLAP,
  FILE_OVERLAP_CHUNK_OVERLAP_PAGE_OVERLAP
}
