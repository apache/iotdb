package cn.edu.thu.tsfiledb.sys.writeLog.transfer;

import java.io.IOException;

import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;

interface Codec<T extends PhysicalPlan> {
	byte[] encode(T t) throws IOException;
	T decode(byte[] bytes) throws IOException;
}
