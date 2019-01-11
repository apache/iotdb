package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.utils.ReadWriteIOUtils;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Digest/statistics per chunk group and per page.
 */
public class TsDigest {

    private Map<String, ByteBuffer> statistics;

    private int serializedSize=Integer.BYTES;

    private int sizeOfList;

    private void reCalculateSerializedSize(){
        serializedSize =Integer.BYTES;
        if(statistics!=null) {
            for (Map.Entry<String, ByteBuffer> entry : statistics.entrySet()) {
                serializedSize += Integer.BYTES + entry.getKey().length() + Integer.BYTES + entry.getValue().remaining();
            }
            sizeOfList = statistics.size();
        }else{
            sizeOfList=0;
        }
    }


    public TsDigest() {
    }


    public void setStatistics(Map<String, ByteBuffer> statistics) {
        this.statistics = statistics;
        reCalculateSerializedSize();
    }

    public Map<String, ByteBuffer> getStatistics() {
        if(statistics == null) return null;
        return Collections.unmodifiableMap(this.statistics);
    }


    public void addStatistics(String key, ByteBuffer value) {
        if (statistics == null) {
            statistics = new HashMap<>();
        }
        statistics.put(key, value);
        serializedSize+=Integer.BYTES+key.length()+Integer.BYTES+value.remaining();
        sizeOfList++;
    }

    @Override
    public String toString() {
        return statistics != null ? statistics.toString() : "";
    }




    public int serializeTo(OutputStream outputStream) throws IOException {
        if((statistics!=null && sizeOfList!=statistics.size())||(statistics==null&&sizeOfList!=0))
            reCalculateSerializedSize();
        int byteLen = 0;
        if(statistics == null|| statistics.size()==0){
            byteLen += ReadWriteIOUtils.write(0, outputStream);// Integer.BYTES;
        } else {
            byteLen += ReadWriteIOUtils.write(statistics.size(), outputStream);//Integer.BYTES;
            for (Map.Entry<String, ByteBuffer> entry : statistics.entrySet()) {
                byteLen += ReadWriteIOUtils.write(entry.getKey(), outputStream);//Integer.BYTES+key.length()
                byteLen += ReadWriteIOUtils.write(entry.getValue(), outputStream);//Integer.BYTES+value.remaining();
            }
        }
        assert byteLen == getSerializedSize();
        return byteLen;
    }

    public int serializeTo(ByteBuffer buffer) {
        if((statistics!=null && sizeOfList!=statistics.size())||(statistics==null&&sizeOfList!=0))
            reCalculateSerializedSize();
        int byteLen = 0;

        if(statistics == null|| statistics.size()==0){
            byteLen += ReadWriteIOUtils.write(0, buffer);// Integer.BYTES;
        } else {
            byteLen += ReadWriteIOUtils.write(statistics.size(), buffer);//Integer.BYTES;
            for (Map.Entry<String, ByteBuffer> entry : statistics.entrySet()) {
                byteLen += ReadWriteIOUtils.write(entry.getKey(), buffer);//Integer.BYTES+key.length()
                byteLen += ReadWriteIOUtils.write(entry.getValue(), buffer);//Integer.BYTES+value.remaining();
            }
        }
        assert byteLen== getSerializedSize();
        return byteLen;
    }

    public static int getNullDigestSize(){
        return Integer.BYTES;
    }

    public static int serializeNullTo(OutputStream outputStream) throws IOException {
        return ReadWriteIOUtils.write(0, outputStream);// Integer.BYTES;
    }

    public static int serializeNullTo(ByteBuffer buffer) {
        return ReadWriteIOUtils.write(0, buffer);// Integer.BYTES;
    }

    public static TsDigest deserializeFrom(InputStream inputStream) throws IOException {
        TsDigest digest = new TsDigest();

        int size = ReadWriteIOUtils.readInt(inputStream);
        if(size > 0) {
            Map<String, ByteBuffer> statistics = new HashMap<>();
            String key;
            ByteBuffer value;
            for (int i = 0; i < size; i++) {
                key = ReadWriteIOUtils.readString(inputStream);
                value = ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(inputStream);
                statistics.put(key, value);
            }
            digest.statistics = statistics;
        }

        return digest;
    }

    public static TsDigest deserializeFrom(ByteBuffer buffer) {
        TsDigest digest = new TsDigest();

        int size = ReadWriteIOUtils.readInt(buffer);
        if(size > 0) {
            Map<String, ByteBuffer> statistics = new HashMap<>();
            String key;
            ByteBuffer value;
            for (int i = 0; i < size; i++) {
                key = ReadWriteIOUtils.readString(buffer);
                value = ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(buffer);
                statistics.put(key, value);
            }
            digest.statistics = statistics;
        }

        return digest;
    }

    public int getSerializedSize() {
        if(statistics==null || (sizeOfList!=statistics.size())){
            reCalculateSerializedSize();
        }
        return serializedSize;
    }
}
