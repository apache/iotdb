package cn.edu.tsinghua.iotdb.writelog.transfer;

import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;

import java.io.IOException;

interface Codec<T extends PhysicalPlan> {

	byte[] encode(T t);

	T decode(byte[] bytes) throws IOException;
}
