package org.apache.iotdb.db.engine.compaction.cross.rewrite.recover;

import org.apache.iotdb.db.engine.compaction.TsFileIdentifier;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.MAGIC_STRING;
import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.STR_SEQ_FILES;
import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.STR_TARGET_FILES;
import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.STR_UNSEQ_FILES;

public class RewriteCrossSpaceCompactionLogAnalyzer {

  private File logFile;
  private List<String> sourceFiles = new ArrayList<>();
  private List<TsFileIdentifier> sourceFileInfos = new ArrayList<>();
  private List<TsFileIdentifier> targetFileInfos = null;
  private String targetFile = null;
  private boolean isSeq = false;

  boolean isAllTmpTargetFilesExisted = false;

  public RewriteCrossSpaceCompactionLogAnalyzer(File logFile) {
    this.logFile = logFile;
  }

  /** @return analyze (source file list, target file) */
  public void analyze() throws IOException {
    String currLine;
    boolean isTargetFile = true;
    List<File> mergeTmpFile = new ArrayList<>();
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(logFile))) {
      int magicCount = 0;
      while ((currLine = bufferedReader.readLine()) != null) {
        switch (currLine) {
          case MAGIC_STRING:
            magicCount++;
            break;
          case STR_TARGET_FILES:
            isTargetFile = true;
            break;
          case STR_SEQ_FILES:
          case STR_UNSEQ_FILES:
            isTargetFile = false;
            break;
          default:
            analyzeFilePath(isTargetFile, currLine, mergeTmpFile, logFile);
            break;
        }
      }
      if (magicCount == 2) {
        isAllTmpTargetFilesExisted = true;
      }
    }
  }

  void analyzeFilePath(
      boolean isTargetFile, String filePath, List<File> mergeTmpFile, File logFile) {
    if (isTargetFile) {
      targetFileInfos.add(TsFileIdentifier.getFileIdentifierFromInfoString(filePath));
    } else {
      sourceFileInfos.add(TsFileIdentifier.getFileIdentifierFromInfoString(filePath));
    }
  }

  public List<String> getSourceFiles() {
    return sourceFiles;
  }

  public List<TsFileIdentifier> getSourceFileInfos() {
    return sourceFileInfos;
  }

  public List<TsFileIdentifier> getTargetFileInfos() {
    return targetFileInfos;
  }

  public boolean isAllTmpTargetFilesExisted() {
    return isAllTmpTargetFilesExisted;
  }

  public String getTargetFile() {
    return targetFile;
  }

  public boolean isSeq() {
    return isSeq;
  }
}
