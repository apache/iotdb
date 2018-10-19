package cn.edu.tsinghua.iotdb.engine.bufferwrite;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.engine.memtable.IMemTable;
import cn.edu.tsinghua.iotdb.engine.memtable.MemTableFlushUtil;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.format.RowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

public class BufferWriteRestoreManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(BufferWriteRestoreManager.class);
    private static final int TS_METADATA_BYTE_SIZE = 4;
    private static final int TS_POSITION_BYTE_SIZE = 8;

    private static final String restoreSuffix = ".restore";
    private static final String DEFAULT_MODE = "rw";
    private Map<String, Map<String, List<TimeSeriesChunkMetaData>>> metadatas;
    private List<RowGroupMetaData> appendRowGroupMetadatas;
    private BufferIO bufferWriteIO;
    private String insertFilePath;
    private String restoreFilePath;
    private String processorName;

    private boolean isNewResource = false;

    public BufferWriteRestoreManager(String processorName, String insertFilePath) throws IOException {
        this.insertFilePath = insertFilePath;
        this.restoreFilePath = insertFilePath + restoreSuffix;
        this.processorName = processorName;
        this.metadatas = new HashMap<>();
        this.appendRowGroupMetadatas = new ArrayList<>();
        recover();
    }

    private void recover() throws IOException {
        File insertFile = new File(insertFilePath);
        File restoreFile = new File(restoreFilePath);
        if (insertFile.exists() && restoreFile.exists()) {
            // read restore file
            Pair<Long, List<RowGroupMetaData>> restoreInfo = readRestoreInfo();
            long position = restoreInfo.left;
            List<RowGroupMetaData> metadatas = restoreInfo.right;
            // cut off tsfile
            FileChannel fileChannel = new FileOutputStream(insertFile, true).getChannel();
            fileChannel.truncate(position);
            fileChannel.close();
            // recovery the BufferWriteIO
            bufferWriteIO = new BufferIO(new TsRandomAccessFileWriter(insertFile), position, metadatas);

            recoverMetadata(metadatas);
            LOGGER.info(
                    "Recover the bufferwrite processor {}, the tsfile path is {}, the position of last flush is {}, the size of rowGroupMetadata is {}",
                    processorName, insertFilePath, position, metadatas.size());
            isNewResource = false;
        } else {
            insertFile.delete();
            restoreFile.delete();
            bufferWriteIO = new BufferIO(new TsRandomAccessFileWriter(insertFile), 0, new ArrayList<>());
            isNewResource = true;
            writeRestoreInfo();
        }
    }

    private void recoverMetadata(List<RowGroupMetaData> rowGroupMetaDatas) {
        for (RowGroupMetaData rowGroupMetaData : rowGroupMetaDatas) {
            String deltaObjectId = rowGroupMetaData.getDeltaObjectID();
            if (!metadatas.containsKey(deltaObjectId)) {
                metadatas.put(deltaObjectId, new HashMap<>());
            }
            for (TimeSeriesChunkMetaData chunkMetaData : rowGroupMetaData.getTimeSeriesChunkMetaDataList()) {
                String measurementId = chunkMetaData.getProperties().getMeasurementUID();
                if (!metadatas.get(deltaObjectId).containsKey(measurementId)) {
                    metadatas.get(deltaObjectId).put(measurementId, new ArrayList<>());
                }
                metadatas.get(deltaObjectId).get(measurementId).add(chunkMetaData);
            }
        }
    }


    private void writeRestoreInfo() throws IOException {
        long lastPosition;
        lastPosition = bufferWriteIO.getPos();
        List<RowGroupMetaData> appendRowGroupMetaDatas = bufferWriteIO.getAppendedRowGroupMetadata();

        //TODO: no need to create a TsRowGroupBlockMetadata, flush RowGroupMetadata one by one is ok
        TsRowGroupBlockMetaData tsRowGroupBlockMetaData = new TsRowGroupBlockMetaData();
        tsRowGroupBlockMetaData.setRowGroups(appendRowGroupMetaDatas);
        RandomAccessFile out = null;
        out = new RandomAccessFile(restoreFilePath, DEFAULT_MODE);
        try {
            if (out.length() > 0) {
                out.seek(out.length() - TS_POSITION_BYTE_SIZE);
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ReadWriteThriftFormatUtils.writeRowGroupBlockMetadata(tsRowGroupBlockMetaData.convertToThrift(), baos);
            // write metadata size using int
            int metadataSize = baos.size();
            out.write(BytesUtils.intToBytes(metadataSize));
            // write metadata
            out.write(baos.toByteArray());
            // write tsfile position using byte[8] which is a long
            byte[] lastPositionBytes = BytesUtils.longToBytes(lastPosition);
            out.write(lastPositionBytes);
        } finally {
            out.close();
        }
    }

    public Pair<Long, List<RowGroupMetaData>> readRestoreInfo() throws IOException {
        byte[] lastPostionBytes = new byte[TS_POSITION_BYTE_SIZE];
        List<RowGroupMetaData> groupMetaDatas = new ArrayList<>();
        RandomAccessFile randomAccessFile = null;
        randomAccessFile = new RandomAccessFile(restoreFilePath, DEFAULT_MODE);
        try {
            long fileLength = randomAccessFile.length();
            // read tsfile position
            long point = randomAccessFile.getFilePointer();
            while (point + TS_POSITION_BYTE_SIZE < fileLength) {
                byte[] metadataSizeBytes = new byte[TS_METADATA_BYTE_SIZE];
                randomAccessFile.read(metadataSizeBytes);
                int metadataSize = BytesUtils.bytesToInt(metadataSizeBytes);
                byte[] thriftBytes = new byte[metadataSize];
                randomAccessFile.read(thriftBytes);
                ByteArrayInputStream inputStream = new ByteArrayInputStream(thriftBytes);
                RowGroupBlockMetaData rowGroupBlockMetaData = ReadWriteThriftFormatUtils
                        .readRowGroupBlockMetaData(inputStream);
                TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
                blockMeta.convertToTSF(rowGroupBlockMetaData);
                groupMetaDatas.addAll(blockMeta.getRowGroups());
                point = randomAccessFile.getFilePointer();
            }
            // read the tsfile position information using byte[8] which is a long.
            randomAccessFile.read(lastPostionBytes);
            long lastPosition = BytesUtils.bytesToLong(lastPostionBytes);
            Pair<Long, List<RowGroupMetaData>> result = new Pair<Long, List<RowGroupMetaData>>(lastPosition,
                    groupMetaDatas);
            return result;
        } finally {
            randomAccessFile.close();
        }
    }

    public List<TimeSeriesChunkMetaData> getInsertMetadatas(String deltaObjectId, String measurementId,
                                                            TSDataType dataType) {
        List<TimeSeriesChunkMetaData> chunkMetaDatas = new ArrayList<>();
        if (metadatas.containsKey(deltaObjectId)) {
            if (metadatas.get(deltaObjectId).containsKey(measurementId)) {
                for (TimeSeriesChunkMetaData chunkMetaData : metadatas.get(deltaObjectId).get(measurementId)) {
                    // filter
                    if (dataType.equals(chunkMetaData.getVInTimeSeriesChunkMetaData().getDataType())) {
                        chunkMetaDatas.add(chunkMetaData);
                    }
                }
            }
        }
        return chunkMetaDatas;
    }

    public String getInsertFilePath() {
        return insertFilePath;
    }

    public String getRestoreFilePath() {
        return restoreFilePath;
    }

    public boolean isNewResource() {
        return isNewResource;
    }

    public void setNewResource(boolean isNewResource) {
        this.isNewResource = isNewResource;
    }

    public void flush(FileSchema fileSchema, IMemTable iMemTable) throws IOException {
        if (iMemTable != null && !iMemTable.isEmpty()) {
            long startPos = bufferWriteIO.getPos();
            long startTime = System.currentTimeMillis();
            // flush data
            MemTableFlushUtil.flushMemTable(fileSchema, bufferWriteIO, iMemTable);
            // write restore information
            writeRestoreInfo();
            long timeInterval = System.currentTimeMillis() - startTime;
            timeInterval = timeInterval == 0 ? 1 : timeInterval;
            long insertSize = bufferWriteIO.getPos() - startPos;
            LOGGER.info(
                    "Bufferwrite processor {} flushes insert data, actual:{}, time consumption:{} ms, flush rate:{}/s",
                    processorName, MemUtils.bytesCntToStr(insertSize), timeInterval,
                    MemUtils.bytesCntToStr(insertSize / timeInterval * 1000));
            appendRowGroupMetadatas.addAll(bufferWriteIO.getAppendedRowGroupMetadata());
        }
    }

    public void appendMetadata() {
        if (!appendRowGroupMetadatas.isEmpty()) {
            for (RowGroupMetaData rowGroupMetaData : appendRowGroupMetadatas) {
                for (TimeSeriesChunkMetaData chunkMetaData : rowGroupMetaData.getTimeSeriesChunkMetaDataList()) {
                    addInsertMetadata(rowGroupMetaData.getDeltaObjectID(),
                            chunkMetaData.getProperties().getMeasurementUID(), chunkMetaData);
                }
            }
            appendRowGroupMetadatas.clear();
        }
    }

    private void addInsertMetadata(String deltaObjectId, String measurementId, TimeSeriesChunkMetaData chunkMetaData) {
        if (!metadatas.containsKey(deltaObjectId)) {
            metadatas.put(deltaObjectId, new HashMap<>());
        }
        if (!metadatas.get(deltaObjectId).containsKey(measurementId)) {
            metadatas.get(deltaObjectId).put(measurementId, new ArrayList<>());
        }
        metadatas.get(deltaObjectId).get(measurementId).add(chunkMetaData);
    }

    public void close(FileSchema fileSchema) throws IOException {
        // call flush and close TsFile
        bufferWriteIO.endFile(fileSchema);
        // delete the restore file
        deleteRestoreFile();
    }

    private void deleteRestoreFile() {
        File restoreFile = new File(restoreFilePath);
        restoreFile.delete();
    }
}
