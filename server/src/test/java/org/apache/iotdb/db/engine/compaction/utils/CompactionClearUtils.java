package org.apache.iotdb.db.engine.compaction.utils;

import org.apache.iotdb.db.engine.compaction.inner.utils.CompactionLogger;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import java.io.File;
import java.io.IOException;

public class CompactionClearUtils {

  /** Clear all generated and merged files in the test directory */
  public static void clearAllCompactionFiles() throws IOException {
    File[] files = FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".tsfile");
    for (File file : files) {
      file.delete();
    }
    File[] resourceFiles =
        FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".resource");
    for (File resourceFile : resourceFiles) {
      resourceFile.delete();
    }
    File[] mergeFiles = FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".tsfile");
    for (File mergeFile : mergeFiles) {
      mergeFile.delete();
    }
    File[] compactionLogFiles =
        FSFactoryProducer.getFSFactory()
            .listFilesBySuffix("target", CompactionLogger.COMPACTION_LOG_NAME);
    for (File compactionLogFile : compactionLogFiles) {
      compactionLogFile.delete();
    }
    File[] modsFiles = FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".mods");
    for (File modsFile : modsFiles) {
      modsFile.delete();
    }
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    FileReaderManager.getInstance().stop();
  }
}
