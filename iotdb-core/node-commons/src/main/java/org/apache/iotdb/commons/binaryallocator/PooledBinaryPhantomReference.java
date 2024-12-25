package org.apache.iotdb.commons.binaryallocator;

import org.apache.iotdb.commons.binaryallocator.arena.Arena;

import org.apache.tsfile.utils.PooledBinary;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;

public class PooledBinaryPhantomReference extends PhantomReference<PooledBinary> {
  public final byte[] byteArray;
  public Arena.SlabRegion slabRegion;

  public PooledBinaryPhantomReference(
      PooledBinary referent,
      ReferenceQueue<? super PooledBinary> q,
      byte[] byteArray,
      Arena.SlabRegion region) {
    super(referent, q);
    this.byteArray = byteArray;
    this.slabRegion = region;
  }
}
