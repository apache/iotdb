/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.utils;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * This class is copied from apache lucene, version 8.4.0. Estimates the size(memory representation)
 * of Java objects. <a
 * href="https://github.com/LuXugang/Lucene-7.x-9.x/blob/master/solr-8.4.0/lucene/core/src/java/org/apache/lucene/util/RamUsageEstimator.java">RamUsageEstimator.java</a>
 * Estimates the size (memory representation) of Java objects.
 *
 * <p>This class uses assumptions that were discovered for the Hotspot virtual machine. If you use a
 * non-OpenJDK/Oracle-based JVM, the measurements may be slightly wrong.
 *
 * @see #shallowSizeOf(Object)
 * @see #shallowSizeOfInstance(Class)
 */
public final class RamUsageEstimator {

  /** One kilobyte bytes. */
  public static final long ONE_KB = 1024;

  /** One megabyte bytes. */
  public static final long ONE_MB = ONE_KB * ONE_KB;

  /** One gigabyte bytes. */
  public static final long ONE_GB = ONE_KB * ONE_MB;

  /** No instantiation. */
  private RamUsageEstimator() {}

  /** True, iff compressed references (oops) are enabled by this JVM */
  public static final boolean COMPRESSED_REFS_ENABLED;

  /** Number of bytes this JVM uses to represent an object reference. */
  public static final int NUM_BYTES_OBJECT_REF;

  /** Number of bytes to represent an object header (no fields, no alignments). */
  public static final int NUM_BYTES_OBJECT_HEADER;

  /** Number of bytes to represent an array header (no content, but with alignments). */
  public static final int NUM_BYTES_ARRAY_HEADER;

  /**
   * A constant specifying the object alignment boundary inside the JVM. Objects will always take a
   * full multiple of this constant, possibly wasting some space.
   */
  public static final int NUM_BYTES_OBJECT_ALIGNMENT;

  /**
   * Approximate memory usage that we assign to all unknown queries - this maps roughly to a
   * BooleanQuery with a couple term clauses.
   */
  public static final int QUERY_DEFAULT_RAM_BYTES_USED = 1024;

  /**
   * Approximate memory usage that we assign to all unknown objects - this maps roughly to a few
   * primitive fields and a couple short String-s.
   */
  public static final int UNKNOWN_DEFAULT_RAM_BYTES_USED = 256;

  /** Sizes of primitive classes. */
  public static final Map<Class<?>, Integer> primitiveSizes;

  static {
    Map<Class<?>, Integer> primitiveSizesMap = new IdentityHashMap<>();
    primitiveSizesMap.put(boolean.class, 1);
    primitiveSizesMap.put(byte.class, 1);
    primitiveSizesMap.put(char.class, Character.BYTES);
    primitiveSizesMap.put(short.class, Short.BYTES);
    primitiveSizesMap.put(int.class, Integer.BYTES);
    primitiveSizesMap.put(float.class, Float.BYTES);
    primitiveSizesMap.put(double.class, Double.BYTES);
    primitiveSizesMap.put(long.class, Long.BYTES);

    primitiveSizes = Collections.unmodifiableMap(primitiveSizesMap);
  }

  /** JVMs typically cache small longs. This tries to find out what the range is. */
  static final long LONG_CACHE_MIN_VALUE, LONG_CACHE_MAX_VALUE;

  static final int LONG_SIZE, STRING_SIZE;

  /** For testing only */
  static final boolean JVM_IS_HOTSPOT_64BIT;

  static final String MANAGEMENT_FACTORY_CLASS = "java.lang.management.ManagementFactory";
  static final String HOTSPOT_BEAN_CLASS = "com.sun.management.HotSpotDiagnosticMXBean";

  // Initialize constants and try to collect information about the JVM internals.
  static {
    if (Constants.JRE_IS_64BIT) {
      // Try to get compressed oops and object alignment (the default seems to be 8 on Hotspot);
      // (this only works on 64 bit, on 32 bits the alignment and reference size is fixed):
      boolean compressedOops = false;
      int objectAlignment = 8;
      boolean isHotspot = false;
      try {
        final Class<?> beanClazz = Class.forName(HOTSPOT_BEAN_CLASS);
        // we use reflection for this, because the management factory is not part
        // of Java 8's compact profile:
        final Object hotSpotBean =
            Class.forName(MANAGEMENT_FACTORY_CLASS)
                .getMethod("getPlatformMXBean", Class.class)
                .invoke(null, beanClazz);
        if (hotSpotBean != null) {
          isHotspot = true;
          final Method getVMOptionMethod = beanClazz.getMethod("getVMOption", String.class);
          try {
            final Object vmOption = getVMOptionMethod.invoke(hotSpotBean, "UseCompressedOops");
            compressedOops =
                Boolean.parseBoolean(
                    vmOption.getClass().getMethod("getValue").invoke(vmOption).toString());
          } catch (ReflectiveOperationException | RuntimeException e) {
            isHotspot = false;
          }
          try {
            final Object vmOption = getVMOptionMethod.invoke(hotSpotBean, "ObjectAlignmentInBytes");
            objectAlignment =
                Integer.parseInt(
                    vmOption.getClass().getMethod("getValue").invoke(vmOption).toString());
          } catch (ReflectiveOperationException | RuntimeException e) {
            isHotspot = false;
          }
        }
      } catch (ReflectiveOperationException | RuntimeException e) {
        isHotspot = false;
      }
      JVM_IS_HOTSPOT_64BIT = isHotspot;
      COMPRESSED_REFS_ENABLED = compressedOops;
      NUM_BYTES_OBJECT_ALIGNMENT = objectAlignment;
      // reference size is 4, if we have compressed oops:
      NUM_BYTES_OBJECT_REF = COMPRESSED_REFS_ENABLED ? 4 : 8;
      // "best guess" based on reference size:
      NUM_BYTES_OBJECT_HEADER = 8 + NUM_BYTES_OBJECT_REF;
      // array header is NUM_BYTES_OBJECT_HEADER + NUM_BYTES_INT, but aligned (object alignment):
      NUM_BYTES_ARRAY_HEADER = (int) alignObjectSize(NUM_BYTES_OBJECT_HEADER + Integer.BYTES);
    } else {
      JVM_IS_HOTSPOT_64BIT = false;
      COMPRESSED_REFS_ENABLED = false;
      NUM_BYTES_OBJECT_ALIGNMENT = 8;
      NUM_BYTES_OBJECT_REF = 4;
      NUM_BYTES_OBJECT_HEADER = 8;
      // For 32 bit JVMs, no extra alignment of array header:
      NUM_BYTES_ARRAY_HEADER = NUM_BYTES_OBJECT_HEADER + Integer.BYTES;
    }

    // get min/max value of cached Long class instances:
    long longCacheMinValue = 0;
    while (longCacheMinValue > Long.MIN_VALUE
        && Long.valueOf(longCacheMinValue - 1) == Long.valueOf(longCacheMinValue - 1)) {
      longCacheMinValue -= 1;
    }
    long longCacheMaxValue = -1;
    while (longCacheMaxValue < Long.MAX_VALUE
        && Long.valueOf(longCacheMaxValue + 1) == Long.valueOf(longCacheMaxValue + 1)) {
      longCacheMaxValue += 1;
    }
    LONG_CACHE_MIN_VALUE = longCacheMinValue;
    LONG_CACHE_MAX_VALUE = longCacheMaxValue;
    LONG_SIZE = (int) shallowSizeOfInstance(Long.class);
    STRING_SIZE = (int) shallowSizeOfInstance(String.class);
  }

  /** Approximate memory usage that we assign to a Hashtable / HashMap entry. */
  public static final long HASHTABLE_RAM_BYTES_PER_ENTRY =
      2
          * NUM_BYTES_OBJECT_REF // key + value
          * 2; // hash tables need to be oversized to avoid collisions, assume 2x capacity

  /** Approximate memory usage that we assign to a LinkedHashMap entry. */
  public static final long LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY =
      HASHTABLE_RAM_BYTES_PER_ENTRY + 2 * NUM_BYTES_OBJECT_REF; // previous & next references

  /** Aligns an object size to be the next multiple of {@link #NUM_BYTES_OBJECT_ALIGNMENT}. */
  public static long alignObjectSize(long size) {
    size += NUM_BYTES_OBJECT_ALIGNMENT - 1L;
    return size - (size % NUM_BYTES_OBJECT_ALIGNMENT);
  }

  /**
   * Return the size of the provided {@link Long} object, returning 0 if it is cached by the JVM and
   * its shallow size otherwise.
   */
  public static long sizeOf(Long value) {
    if (value >= LONG_CACHE_MIN_VALUE && value <= LONG_CACHE_MAX_VALUE) {
      return 0;
    }
    return LONG_SIZE;
  }

  /** Returns the size in bytes of the byte[] object. */
  public static long sizeOf(byte[] arr) {
    return arr == null ? 0 : alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + arr.length);
  }

  /** Returns the size in bytes of the boolean[] object. */
  public static long sizeOf(boolean[] arr) {
    return arr == null ? 0 : alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + arr.length);
  }

  /** Returns the size in bytes of the char[] object. */
  public static long sizeOf(char[] arr) {
    return arr == null
        ? 0
        : alignObjectSize(NUM_BYTES_ARRAY_HEADER + (long) Character.BYTES * arr.length);
  }

  /** Returns the size in bytes of the short[] object. */
  public static long sizeOf(short[] arr) {
    return arr == null
        ? 0
        : alignObjectSize(NUM_BYTES_ARRAY_HEADER + (long) Short.BYTES * arr.length);
  }

  /** Returns the size in bytes of the int[] object. */
  public static long sizeOf(int[] arr) {
    return arr == null
        ? 0
        : alignObjectSize(NUM_BYTES_ARRAY_HEADER + (long) Integer.BYTES * arr.length);
  }

  /** Returns the size in bytes of the float[] object. */
  public static long sizeOf(float[] arr) {
    return arr == null
        ? 0
        : alignObjectSize(NUM_BYTES_ARRAY_HEADER + (long) Float.BYTES * arr.length);
  }

  /** Returns the size in bytes of the long[] object. */
  public static long sizeOf(long[] arr) {
    return arr == null
        ? 0
        : alignObjectSize(NUM_BYTES_ARRAY_HEADER + (long) Long.BYTES * arr.length);
  }

  /** Returns the size in bytes of the double[] object. */
  public static long sizeOf(double[] arr) {
    return arr == null
        ? 0
        : alignObjectSize(NUM_BYTES_ARRAY_HEADER + (long) Double.BYTES * arr.length);
  }

  /** Returns the size in bytes of the String[] object. */
  public static long sizeOf(String[] arr) {
    long size = shallowSizeOf(arr);
    for (String s : arr) {
      if (s == null) {
        continue;
      }
      size += sizeOf(s);
    }
    return size;
  }

  /** Recurse only into immediate descendants. */
  public static final int MAX_DEPTH = 1;

  /**
   * Returns the size in bytes of a Map object, including sizes of its keys and values, supplying
   * {@link #UNKNOWN_DEFAULT_RAM_BYTES_USED} when object type is not well known. This method
   * recurses up to {@link #MAX_DEPTH}.
   */
  public static long sizeOfMap(Map<?, ?> map) {
    return sizeOfMap(map, 0, UNKNOWN_DEFAULT_RAM_BYTES_USED);
  }

  /**
   * Returns the size in bytes of a Map object, including sizes of its keys and values, supplying
   * default object size when object type is not well known. This method recurses up to {@link
   * #MAX_DEPTH}.
   */
  public static long sizeOfMap(Map<?, ?> map, long defSize) {
    return sizeOfMap(map, 0, defSize);
  }

  private static long sizeOfMap(Map<?, ?> map, int depth, long defSize) {
    if (map == null) {
      return 0;
    }
    long size = shallowSizeOf(map);
    if (depth > MAX_DEPTH) {
      return size;
    }
    long sizeOfEntry = -1;
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (sizeOfEntry == -1) {
        sizeOfEntry = shallowSizeOf(entry);
      }
      size += sizeOfEntry;
      size += sizeOfObject(entry.getKey(), depth, defSize);
      size += sizeOfObject(entry.getValue(), depth, defSize);
    }
    return alignObjectSize(size);
  }

  /**
   * Returns the size in bytes of a Collection object, including sizes of its values, supplying
   * {@link #UNKNOWN_DEFAULT_RAM_BYTES_USED} when object type is not well known. This method
   * recurses up to {@link #MAX_DEPTH}.
   */
  public static long sizeOfCollection(Collection<?> collection) {
    return sizeOfCollection(collection, 0, UNKNOWN_DEFAULT_RAM_BYTES_USED);
  }

  /**
   * Returns the size in bytes of a Collection object, including sizes of its values, supplying
   * default object size when object type is not well known. This method recurses up to {@link
   * #MAX_DEPTH}.
   */
  public static long sizeOfCollection(Collection<?> collection, long defSize) {
    return sizeOfCollection(collection, 0, defSize);
  }

  private static long sizeOfCollection(Collection<?> collection, int depth, long defSize) {
    if (collection == null) {
      return 0;
    }
    long size = shallowSizeOf(collection);
    if (depth > MAX_DEPTH) {
      return size;
    }
    // assume array-backed collection and add per-object references
    size += NUM_BYTES_ARRAY_HEADER + (long) collection.size() * NUM_BYTES_OBJECT_REF;
    for (Object o : collection) {
      size += sizeOfObject(o, depth, defSize);
    }
    return alignObjectSize(size);
  }

  /**
   * Best effort attempt to estimate the size in bytes of an undetermined object. Known types will
   * be estimated according to their formulas, and all other object sizes will be estimated as
   * {@link #UNKNOWN_DEFAULT_RAM_BYTES_USED}.
   */
  public static long sizeOfObject(Object o) {
    return sizeOfObject(o, 0, UNKNOWN_DEFAULT_RAM_BYTES_USED);
  }

  /**
   * Best effort attempt to estimate the size in bytes of an undetermined object. Known types will
   * be estimated according to their formulas, and all other object sizes will be estimated using
   * {@link #shallowSizeOf(Object)}, or using the supplied <code>defSize</code> parameter if its
   * value is greater than 0.
   */
  public static long sizeOfObject(Object o, long defSize) {
    return sizeOfObject(o, 0, defSize);
  }

  private static long sizeOfObject(Object o, int depth, long defSize) {
    if (o == null) {
      return 0;
    }
    long size;
    if (o instanceof Accountable) {
      size = ((Accountable) o).ramBytesUsed();
    } else if (o instanceof String) {
      size = sizeOf((String) o);
    } else if (o instanceof boolean[]) {
      size = sizeOf((boolean[]) o);
    } else if (o instanceof byte[]) {
      size = sizeOf((byte[]) o);
    } else if (o instanceof char[]) {
      size = sizeOf((char[]) o);
    } else if (o instanceof double[]) {
      size = sizeOf((double[]) o);
    } else if (o instanceof float[]) {
      size = sizeOf((float[]) o);
    } else if (o instanceof int[]) {
      size = sizeOf((int[]) o);
    } else if (o instanceof Long) {
      size = sizeOf((Long) o);
    } else if (o instanceof long[]) {
      size = sizeOf((long[]) o);
    } else if (o instanceof short[]) {
      size = sizeOf((short[]) o);
    } else if (o instanceof String[]) {
      size = sizeOf((String[]) o);
    } else if (o instanceof Map) {
      size = sizeOfMap((Map) o, ++depth, defSize);
    } else if (o instanceof Collection) {
      size = sizeOfCollection((Collection) o, ++depth, defSize);
    } else {
      if (defSize > 0) {
        size = defSize;
      } else {
        size = shallowSizeOf(o);
      }
    }
    return size;
  }

  /** Returns the size in bytes of the String object. */
  public static long sizeOf(String s) {
    if (s == null) {
      return 0;
    }
    // may not be true in Java 9+ and CompactStrings - but we have no way to determine this

    // char[] + hashCode
    long size = STRING_SIZE + (long) NUM_BYTES_ARRAY_HEADER + (long) Character.BYTES * s.length();
    return alignObjectSize(size);
  }

  /** Returns the shallow size in bytes of the Object[] object. */
  // Use this method instead of #shallowSizeOf(Object) to avoid costly reflection
  public static long shallowSizeOf(Object[] arr) {
    return arr == null
        ? 0
        : alignObjectSize(NUM_BYTES_ARRAY_HEADER + (long) NUM_BYTES_OBJECT_REF * arr.length);
  }

  /**
   * Estimates a "shallow" memory usage of the given object. For arrays, this will be the memory
   * taken by array storage (no subreferences will be followed). For objects, this will be the
   * memory taken by the fields.
   *
   * <p>JVM object alignments are also applied.
   */
  public static long shallowSizeOf(Object obj) {
    if (obj == null) return 0;
    final Class<?> clz = obj.getClass();
    if (clz.isArray()) {
      return shallowSizeOfArray(obj);
    } else {
      return shallowSizeOfInstance(clz);
    }
  }

  /**
   * Returns the shallow instance size in bytes an instance of the given class would occupy. This
   * works with all conventional classes and primitive types, but not with arrays (the size then
   * depends on the number of elements and varies from object to object).
   *
   * @see #shallowSizeOf(Object)
   * @throws IllegalArgumentException if {@code clazz} is an array class.
   */
  public static long shallowSizeOfInstance(Class<?> clazz) {
    if (clazz.isArray())
      throw new IllegalArgumentException("This method does not work with array classes.");
    if (clazz.isPrimitive()) return primitiveSizes.get(clazz);

    long size = NUM_BYTES_OBJECT_HEADER;

    // Walk type hierarchy
    for (; clazz != null; clazz = clazz.getSuperclass()) {
      final Field[] fields =
          AccessController.doPrivileged((PrivilegedAction<Field[]>) clazz::getDeclaredFields);
      for (Field f : fields) {
        if (!Modifier.isStatic(f.getModifiers())) {
          size = adjustForField(size, f);
        }
      }
    }
    return alignObjectSize(size);
  }

  /** Return shallow size of any <code>array</code>. */
  private static long shallowSizeOfArray(Object array) {
    long size = NUM_BYTES_ARRAY_HEADER;
    final int len = Array.getLength(array);
    if (len > 0) {
      Class<?> arrayElementClazz = array.getClass().getComponentType();
      if (arrayElementClazz.isPrimitive()) {
        size += (long) len * primitiveSizes.get(arrayElementClazz);
      } else {
        size += (long) NUM_BYTES_OBJECT_REF * len;
      }
    }
    return alignObjectSize(size);
  }

  /**
   * This method returns the maximum representation size of an object. <code>sizeSoFar</code> is the
   * object's size measured so far. <code>f</code> is the field being probed.
   *
   * <p>The returned offset will be the maximum of whatever was measured so far and <code>f</code>
   * field's offset and representation size (unaligned).
   */
  static long adjustForField(long sizeSoFar, final Field f) {
    final Class<?> type = f.getType();
    final int fsize = type.isPrimitive() ? primitiveSizes.get(type) : NUM_BYTES_OBJECT_REF;
    // TODO: No alignments based on field type/ subclass fields alignments?
    return sizeSoFar + fsize;
  }

  /** Returns <code>size</code> in human-readable units (GB, MB, KB or bytes). */
  public static String humanReadableUnits(long bytes) {
    return humanReadableUnits(
        bytes, new DecimalFormat("0.#", DecimalFormatSymbols.getInstance(Locale.ROOT)));
  }

  /** Returns <code>size</code> in human-readable units (GB, MB, KB or bytes). */
  public static String humanReadableUnits(long bytes, DecimalFormat df) {
    if (bytes / ONE_GB > 0) {
      return df.format((float) bytes / ONE_GB) + " GB";
    } else if (bytes / ONE_MB > 0) {
      return df.format((float) bytes / ONE_MB) + " MB";
    } else if (bytes / ONE_KB > 0) {
      return df.format((float) bytes / ONE_KB) + " KB";
    } else {
      return bytes + " bytes";
    }
  }

  public static long sizeOfBooleanArray(int length) {
    return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + length);
  }

  public static long sizeOfByteArray(int length) {
    return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + length);
  }

  public static long sizeOfShortArray(int length) {
    return alignObjectSize(NUM_BYTES_ARRAY_HEADER + (long) Short.BYTES * length);
  }

  public static long sizeOfCharArray(int length) {
    return alignObjectSize(NUM_BYTES_ARRAY_HEADER + (long) Character.BYTES * length);
  }

  public static long sizeOfIntArray(int length) {
    return alignObjectSize(NUM_BYTES_ARRAY_HEADER + (long) Integer.BYTES * length);
  }

  public static long sizeOfLongArray(int length) {
    return alignObjectSize(NUM_BYTES_ARRAY_HEADER + (long) Long.BYTES * length);
  }

  public static long sizeOfFloatArray(int length) {
    return alignObjectSize(NUM_BYTES_ARRAY_HEADER + (long) Float.BYTES * length);
  }

  public static long sizeOfDoubleArray(int length) {
    return alignObjectSize(NUM_BYTES_ARRAY_HEADER + (long) Double.BYTES * length);
  }

  public static long sizeOfObjectArray(int length) {
    return alignObjectSize(NUM_BYTES_ARRAY_HEADER + (long) NUM_BYTES_OBJECT_REF * length);
  }
}
