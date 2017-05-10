package cn.edu.thu.tsfiledb.sys.writeLog;

import java.io.IOException;

import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;

interface Codec<T extends PhysicalPlan> {
	byte[] encode(T t) throws IOException;
	T decode(byte[] bytes) throws IOException;
}
