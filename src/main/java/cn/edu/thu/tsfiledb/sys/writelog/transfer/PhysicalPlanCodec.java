package cn.edu.thu.tsfiledb.sys.writelog.transfer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import cn.edu.thu.tsfile.common.utils.BytesUtils;
import cn.edu.thu.tsfile.common.utils.ReadWriteStreamUtils;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.physical.crud.DeletePlan;
import cn.edu.thu.tsfiledb.qp.physical.crud.InsertPlan;
import cn.edu.thu.tsfiledb.qp.physical.crud.UpdatePlan;

/**
 * @author CGF
 */
public enum PhysicalPlanCodec {

    MULTIINSERTPLAN(OperatorType.INSERT.ordinal(), codecInstances.multiInsertPlanCodec),
    UPDATEPLAN(OperatorType.UPDATE.ordinal(), codecInstances.updatePlanCodec),
    DELETEPLAN(OperatorType.DELETE.ordinal(), codecInstances.deletePlanCodec);

    public final int planCode;
    public final Codec<?> codec;

    PhysicalPlanCodec(int planCode, Codec<?> codec) {
        this.planCode = planCode;
        this.codec = codec;
    }

    private static final HashMap<Integer, PhysicalPlanCodec> codecMap = new HashMap<>();

    static {
        for (PhysicalPlanCodec codec : PhysicalPlanCodec.values()) {
            codecMap.put(codec.planCode, codec);
        }
    }

    public static PhysicalPlanCodec fromOpcode(int opcode) {
        if (!codecMap.containsKey(opcode)) {
            throw new UnsupportedOperationException("opcode given is not supported. " + opcode);
        }
        return codecMap.get(opcode);
    }

    static class codecInstances {

        static final Codec<DeletePlan> deletePlanCodec = new Codec<DeletePlan>() {

            @Override
            public byte[] encode(DeletePlan t) {
                int type = OperatorType.DELETE.ordinal();
                byte[] timeBytes = BytesUtils.longToBytes(t.getDeleteTime());

                byte[] pathBytes = BytesUtils.StringToBytes(t.getPaths().get(0).getFullPath());
                byte[] pathBytesLength = ReadWriteStreamUtils.getUnsignedVarInt(pathBytes.length);

                int totalLength = 1 + timeBytes.length + pathBytes.length + pathBytesLength.length;

                byte[] res = new byte[totalLength];
                int pos = 0;
                res[0] = (byte) type;
                pos += 1;
                System.arraycopy(timeBytes, 0, res, pos, timeBytes.length);
                pos += timeBytes.length;

                System.arraycopy(pathBytesLength, 0, res, pos, pathBytesLength.length);
                pos += pathBytesLength.length;
                System.arraycopy(pathBytes, 0, res, pos, pathBytes.length);
                pos += pathBytes.length;

                return res;
            }

            @Override
            public DeletePlan decode(byte[] bytes) throws IOException {
                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

                int type = bais.read();
                byte[] timeBytes = new byte[8];
                bais.read(timeBytes, 0, 8);
                long time = BytesUtils.bytesToLong(timeBytes);

                int pathLength = ReadWriteStreamUtils.readUnsignedVarInt(bais);
                byte[] pathBytes = new byte[pathLength];
                bais.read(pathBytes, 0, pathLength);
                String path = BytesUtils.bytesToString(pathBytes);

                return new DeletePlan(time, new Path(path));
            }
        };

        static final Codec<UpdatePlan> updatePlanCodec = new Codec<UpdatePlan>() {

            @Override
            public byte[] encode(UpdatePlan updatePlan) throws IOException {
                int type = OperatorType.UPDATE.ordinal();
                byte[] startTimeBytes = BytesUtils.longToBytes(updatePlan.getStartTime());
                byte[] endTimeBytes = BytesUtils.longToBytes(updatePlan.getEndTime());

                byte[] valueBytes = BytesUtils.StringToBytes(updatePlan.getValue());
                byte[] valueBytesLength = ReadWriteStreamUtils.getUnsignedVarInt(valueBytes.length);

                byte[] pathBytes = BytesUtils.StringToBytes(updatePlan.getPath().getFullPath());
                byte[] pathBytesLength = ReadWriteStreamUtils.getUnsignedVarInt(pathBytes.length);

                int totalLength = 1 + startTimeBytes.length + endTimeBytes.length +
                        valueBytesLength.length + valueBytes.length + pathBytes.length + pathBytesLength.length;;

                byte[] res = new byte[totalLength];
                int pos = 0;
                res[0] = (byte) type;
                pos += 1;
                System.arraycopy(startTimeBytes, 0, res, pos, startTimeBytes.length);
                pos += startTimeBytes.length;
                System.arraycopy(endTimeBytes, 0, res, pos, endTimeBytes.length);
                pos += endTimeBytes.length;

                System.arraycopy(valueBytesLength, 0, res, pos, valueBytesLength.length);
                pos += valueBytesLength.length;
                System.arraycopy(valueBytes, 0, res, pos, valueBytes.length);
                pos += valueBytes.length;

                System.arraycopy(pathBytesLength, 0, res, pos, pathBytesLength.length);
                pos += pathBytesLength.length;
                System.arraycopy(pathBytes, 0, res, pos, pathBytes.length);
                pos += pathBytes.length;

                return res;
            }

            @Override
            public UpdatePlan decode(byte[] bytes) throws IOException {
                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

                int type = bais.read();
                byte[] startTimeBytes = new byte[8];
                bais.read(startTimeBytes, 0, 8);
                long startTime = BytesUtils.bytesToLong(startTimeBytes);
                byte[] endTimeBytes = new byte[8];
                bais.read(endTimeBytes, 0, 8);
                long endTime = BytesUtils.bytesToLong(endTimeBytes);

                int valueLength = ReadWriteStreamUtils.readUnsignedVarInt(bais);
                byte[] valueBytes = new byte[valueLength];
                bais.read(valueBytes, 0, valueLength);
                String value = BytesUtils.bytesToString(valueBytes);

                int pathLength = ReadWriteStreamUtils.readUnsignedVarInt(bais);
                byte[] pathBytes = new byte[pathLength];
                bais.read(pathBytes, 0, pathLength);
                String path = BytesUtils.bytesToString(pathBytes);

                return new UpdatePlan(startTime, endTime, value, new Path(path));
            }
        };

        static final Codec<InsertPlan> multiInsertPlanCodec = new Codec<InsertPlan>() {
            @Override
            public byte[] encode(InsertPlan t) {
                int type = OperatorType.INSERT.ordinal();
                int insertType = t.getInsertType();
                byte[] timeBytes = BytesUtils.longToBytes(t.getTime());
                byte[] deltaObjectBytes = BytesUtils.StringToBytes(t.getDeltaObject());
                byte[] deltaObjectLengthBytes = ReadWriteStreamUtils.getUnsignedVarInt(deltaObjectBytes.length);

                int allLen = 0, mLen = 0, pos = 0;
                List<byte[]> measurementBytesList = new ArrayList<>();
                List<String> measurementList = t.getMeasurements();
                for (String m : measurementList) {
                    byte[] mBytes = BytesUtils.StringToBytes(m);
                    byte[] lenBytes = ReadWriteStreamUtils.getUnsignedVarInt(mBytes.length);
                    allLen += lenBytes.length;
                    allLen += mBytes.length;
                    byte[] tmpBytes = new byte[lenBytes.length + mBytes.length];
                    pos = 0;
                    System.arraycopy(lenBytes, 0, tmpBytes, pos, lenBytes.length);
                    pos += lenBytes.length;
                    System.arraycopy(mBytes, 0, tmpBytes, pos, mBytes.length);
                    pos += mBytes.length;
                    measurementBytesList.add(tmpBytes);
                }
                pos = 0;
                byte[] mmAllBytes = new byte[allLen];
                for (byte[] x : measurementBytesList) {
                    System.arraycopy(x, 0, mmAllBytes, pos, x.length);
                    pos += x.length;
                }
                byte[] preMmBytes = ReadWriteStreamUtils.getUnsignedVarInt(mmAllBytes.length);

                allLen = 0;
                List<byte[]> valueBytesList = new ArrayList<>();
                List<String> valueList = t.getValues();
                for (String m : valueList) {
                    byte[] vBytes = BytesUtils.StringToBytes(m);
                    byte[] lenBytes = ReadWriteStreamUtils.getUnsignedVarInt(vBytes.length);
                    allLen += lenBytes.length;
                    allLen += vBytes.length;
                    byte[] tmpBytes = new byte[lenBytes.length + vBytes.length];
                    pos = 0;
                    System.arraycopy(lenBytes, 0, tmpBytes, pos, lenBytes.length);
                    pos += lenBytes.length;
                    System.arraycopy(vBytes, 0, tmpBytes, pos, vBytes.length);
                    pos += vBytes.length;
                    valueBytesList.add(tmpBytes);
                }
                pos = 0;
                byte[] valueAllBytes = new byte[allLen];
                for (byte[] x : valueBytesList) {
                    System.arraycopy(x, 0, valueAllBytes, pos, x.length);
                    pos += x.length;
                }
                byte[] preValueBytes = ReadWriteStreamUtils.getUnsignedVarInt(valueAllBytes.length);

                int totalLength = 1 + 1 + timeBytes.length + deltaObjectLengthBytes.length + deltaObjectBytes.length
                        + preMmBytes.length + mmAllBytes.length + preValueBytes.length + valueAllBytes.length;

                byte[] res = new byte[totalLength];
                pos = 0;
                res[0] = (byte) type;
                res[1] = (byte) insertType;
                pos += 2;

                System.arraycopy(timeBytes, 0, res, pos, timeBytes.length);
                pos += timeBytes.length;

                System.arraycopy(deltaObjectLengthBytes, 0, res, pos, deltaObjectLengthBytes.length);
                pos += deltaObjectLengthBytes.length;
                System.arraycopy(deltaObjectBytes, 0, res, pos, deltaObjectBytes.length);
                pos += deltaObjectBytes.length;

                System.arraycopy(preMmBytes, 0, res, pos, preMmBytes.length);
                pos += preMmBytes.length;
                System.arraycopy(mmAllBytes, 0, res, pos, mmAllBytes.length);
                pos += mmAllBytes.length;

                System.arraycopy(preValueBytes, 0, res, pos, preValueBytes.length);
                pos += preValueBytes.length;
                System.arraycopy(valueAllBytes, 0, res, pos, valueAllBytes.length);
                pos += valueAllBytes.length;

                return res;
            }

            @Override
            public InsertPlan decode(byte[] bytes) throws IOException {
                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

                int type = bais.read();
                int insertType = bais.read();
                byte[] timeBytes = new byte[8];
                bais.read(timeBytes, 0, 8);
                long time = BytesUtils.bytesToLong(timeBytes);

                int deltaObjectLen = ReadWriteStreamUtils.readUnsignedVarInt(bais);
                byte[] deltaObjectBytes = new byte[deltaObjectLen];
                bais.read(deltaObjectBytes, 0, deltaObjectLen);
                String deltaObject = BytesUtils.bytesToString(deltaObjectBytes);

                int mmListLength = ReadWriteStreamUtils.readUnsignedVarInt(bais);
                byte[] mmListBytes = new byte[mmListLength];
                bais.read(mmListBytes, 0, mmListLength);
                List<String> measurementsList = new ArrayList<>();
                ByteArrayInputStream mmBais = new ByteArrayInputStream(mmListBytes);
                while (mmBais.available() > 0) {
                    int mmLen = ReadWriteStreamUtils.readUnsignedVarInt(mmBais);
                    byte[] mmBytes = new byte[mmLen];
                    mmBais.read(mmBytes, 0, mmLen);
                    measurementsList.add(BytesUtils.bytesToString(mmBytes));
                }

                int valueListLength = ReadWriteStreamUtils.readUnsignedVarInt(bais);
                byte[] valueListBytes = new byte[valueListLength];
                bais.read(valueListBytes, 0, valueListLength);
                List<String> valuesList = new ArrayList<>();
                ByteArrayInputStream valueBais = new ByteArrayInputStream(valueListBytes);
                while (valueBais.available() > 0) {
                    int valueLen = ReadWriteStreamUtils.readUnsignedVarInt(valueBais);
                    byte[] valueBytes = new byte[valueLen];
                    valueBais.read(valueBytes, 0, valueLen);
                    valuesList.add(BytesUtils.bytesToString(valueBytes));
                }

                InsertPlan ans = new InsertPlan(deltaObject, time, measurementsList, valuesList);
                return ans;
            }
        };

    }
}