package cn.edu.tsinghua.iotdb.sys.writelog.transfer;

import java.io.IOException;

import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;

interface Codec<T extends PhysicalPlan> {

	byte[] encode(T t) throws IOException;

	T decode(byte[] bytes) throws IOException;
}
