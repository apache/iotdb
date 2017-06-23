package cn.edu.thu.tsfiledb.sys.writeLog.transfer;

import java.io.IOException;

import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;

public class PhysicalPlanLogTransfer {

    public static byte[] operatorToLog(PhysicalPlan plan) throws IOException {
        Codec<PhysicalPlan> codec = (Codec<PhysicalPlan>) PhysicalPlanCodec.fromOpcode(plan.getOperatorType().ordinal()).codec;
        return codec.encode(plan);
    }

    public static PhysicalPlan logToOperator(byte[] opInBytes) throws IOException {
        // the first byte determines the opCode
        int opCode = opInBytes[0];
        Codec<PhysicalPlan> codec = (Codec<PhysicalPlan>) PhysicalPlanCodec.fromOpcode(opCode).codec;
        return codec.decode(opInBytes);
    }
}
