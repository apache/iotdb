package org.apache.iotdb.db.engine.alter.log;

import org.apache.iotdb.db.engine.compaction.log.TsFileIdentifier;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * altering log analyzer <br/>
 * Find incomplete partition list <br/>
 * find list of incomplete tsfiles <br/>
 * Incomplete tsfile status: <br/>
 * 1. There is no .tsfile <br/>
 * 1.1, .alter.old exists and .alter exists - wait for completion <br/>
 * 1.2, only .alter.old exists - system exception <br/>
 * 1.2, only exists .alter - system exception <br/>
 * 2. There is .tsfile <br/>
 * 2.1, exists.alter - writing <br/>
 * 2.2, does not exist.alter - not started <br/>
 **/
public class AlteringLogAnalyzer {

    private final File alterLogFile;

    public AlteringLogAnalyzer(File alterLogFile) {
        this.alterLogFile = alterLogFile;
    }

    public Map<String, Set<TsFileIdentifier>> analyzer() throws IOException {

        final Map<String, Set<TsFileIdentifier>> undoPartitionTsFiles = new HashMap<>();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(alterLogFile))) {
            // file header
            String fullPath = bufferedReader.readLine();
            if(fullPath == null) {
                throw new IOException("alter.log parse fail, fullPath is null");
            }
            TSEncoding encoding = TSEncoding.deserialize(Byte.parseByte(bufferedReader.readLine()));
            if(encoding == null) {
                throw new IOException("alter.log parse fail, encoding is null");
            }
            CompressionType compressionType = CompressionType.deserialize(Byte.parseByte(bufferedReader.readLine()));
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
                undoPartitionTsFiles.put(partition + TsFileIdentifier.INFO_SEPARATOR + AlertingLogger.FLAG_SEQ, new HashSet<>());
                undoPartitionTsFiles.put(partition + TsFileIdentifier.INFO_SEPARATOR + AlertingLogger.FLAG_UNSEQ, new HashSet<>());
            }
            Long readCurPartition;
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
                String key = readCurPartition + TsFileIdentifier.INFO_SEPARATOR + partitionHeaders[2];
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
}
