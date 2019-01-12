package org.apache.iotdb.db.writelog.transfer;

import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.IOException;

interface Codec<T extends PhysicalPlan> {

	byte[] encode(T t);

	T decode(byte[] bytes) throws IOException;
}
