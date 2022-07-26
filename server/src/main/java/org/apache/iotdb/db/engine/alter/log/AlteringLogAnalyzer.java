package org.apache.iotdb.db.engine.alter.log;

import org.apache.iotdb.db.engine.compaction.log.TsFileIdentifier;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * altering log analyzer
 **/
public class AlteringLogAnalyzer {

    private final File alterLogFile;

    private String fullPath;

    private TSEncoding encoding;

    private CompressionType compressionType;

    public AlteringLogAnalyzer(File alterLogFile) {
        this.alterLogFile = alterLogFile;
    }

    public Map<Pair<Long, Boolean>, Set<TsFileIdentifier>> analyzer() throws IOException {

        final Map<Pair<Long, Boolean>, Set<TsFileIdentifier>> undoPartitionTsFiles = new HashMap<>();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(alterLogFile))) {
            // file header
            fullPath = bufferedReader.readLine();
            if(fullPath == null) {
                throw new IOException("alter.log parse fail, fullPath is null");
            }
            encoding = TSEncoding.deserialize(Byte.parseByte(bufferedReader.readLine()));
            if(encoding == null) {
                throw new IOException("alter.log parse fail, encoding is null");
            }
            compressionType = CompressionType.deserialize(Byte.parseByte(bufferedReader.readLine()));
            if(compressionType == null) {
                throw new IOException("alter.log parse fail, compressionType is null");
            }
            // partition list
            String curPartition;
            while (true) {
                curPartition = bufferedReader.readLine();
                if(curPartition == null) {
                    return undoPartitionTsFiles;
                }
                if(curPartition.equals(AlertingLogger.FLAG_TIME_PARTITIONS_HEADER_DONE)) {
                    break;
                }
                Long partition = Long.parseLong(curPartition);
                undoPartitionTsFiles.put(new Pair<>(partition, true), new HashSet<>());
                undoPartitionTsFiles.put(new Pair<>(partition, false), new HashSet<>());
            }
            long readCurPartition;
            while(!undoPartitionTsFiles.isEmpty()) {
                // partition header
                // ftps partition isSeq
                String partitionHeaderStr = bufferedReader.readLine();
                if(partitionHeaderStr == null || !partitionHeaderStr.startsWith(AlertingLogger.FLAG_TIME_PARTITION_START)) {
                    throw new IOException("alter.log parse fail, line not start with FLAG_TIME_PARTITION_START");
                }
                String[] partitionHeaders = partitionHeaderStr.split(TsFileIdentifier.INFO_SEPARATOR);
                if(partitionHeaders.length != 3) {
                    throw new IOException("alter.log parse fail, partitionHeaders error");
                }
                readCurPartition = Long.parseLong(partitionHeaders[1]);
                if(!AlertingLogger.FLAG_SEQ.equals(partitionHeaders[2]) && !AlertingLogger.FLAG_UNSEQ.equals(partitionHeaders[2])) {
                    throw new IOException("alter.log parse fail, partitionHeaders error");
                }
                Pair<Long, Boolean> key = new Pair<>(readCurPartition, AlertingLogger.FLAG_SEQ.equals(partitionHeaders[2]));
                Set<TsFileIdentifier> tsFileIdentifiers = undoPartitionTsFiles.get(key);
                String curLineStr;
                while (true) {
                    // read tsfile list
                    curLineStr = bufferedReader.readLine();
                    if(curLineStr == null) {
                        return undoPartitionTsFiles;
                    }
                    if(curLineStr.equals(AlertingLogger.FLAG_TIME_PARTITION_DONE)) {
                        undoPartitionTsFiles.remove(key);
                        break;
                    }
                    if(curLineStr.startsWith(AlertingLogger.FLAG_INIT_SELECTED_FILE)) {
                        // add tsfile list
                        tsFileIdentifiers.add(TsFileIdentifier.getFileIdentifierFromOldInfoString(curLineStr.substring(AlertingLogger.FLAG_INIT_SELECTED_FILE.length() + TsFileIdentifier.INFO_SEPARATOR.length())));
                    } else if(curLineStr.startsWith(AlertingLogger.FLAG_DONE)) {
                        // remove done file
                        tsFileIdentifiers.remove(TsFileIdentifier.getFileIdentifierFromOldInfoString(curLineStr.substring(AlertingLogger.FLAG_DONE.length() + TsFileIdentifier.INFO_SEPARATOR.length())));
                    } else {
                        throw new IOException("alter.log parse fail, unknown line");
                    }
                }
            }
        }
        return undoPartitionTsFiles;
    }

    public String getFullPath() {
        return fullPath;
    }

    public TSEncoding getEncoding() {
        return encoding;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }
}
