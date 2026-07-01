package org.apache.iotdb.tsfile.encoding.elf.chimp;

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.booleans.BooleanIterator;
import it.unimi.dsi.fastutil.io.RepositionableStream;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.FileChannel;


/** Bit-level output stream.
 *
 * <P>This class wraps any {@link OutputStream} so that you can treat it as
 * <em>bit</em> stream. Constructors and methods closely resemble those of
 * {@link OutputStream}. Data can be added to such a stream in several ways:
 * writing an integer or long in fixed-width, unary, &gamma;, &delta;, &zeta; and Golomb
 * coding, or providing a vector of bytes.
 *
 * <P>This class can also {@linkplain #ElfOutputBitStream(byte[]) wrap a byte
 * array}; this is much more lightweight than wrapping a {@link
 * it.unimi.dsi.fastutil.io.FastByteArrayOutputStream} wrapping the array, but overflowing the array
 * will cause an {@link IOException}.
 *
 * <P>Note that when writing using a vector of bytes bits are written in the natural
 * way: the first bit is bit 7 of the first byte, the eighth bit is bit 0 of
 * the first byte, the ninth bit is bit 7 of the second byte and so on. When
 * writing integers using some coding, instead, the <em>lower</em> bits are considered
 * for coding (in the fixed-width case, the given number of bits, otherwise
 * the lower bits starting from the most significant one).
 *
 * <h2>The bit stream format</h2>
 *
 * <P>The bit streams written by this class are <em>big endian</em>. That is,
 * the first bit of the stream is bit 7 of the first byte, the eightth bit
 * is bit 0 of the first byte, the ninth bit is bit 7 of the second byte and so on.
 *
 * <P>Blocks of bits (such as coded integers) are written <em>starting from the
 * most significant bit</em>. In other words, if you take the first bytes of a stream
 * and print them in binary you will see exactly the sequence of bits you have
 * written. In particular, if you write 32-bit integers you will get a stream
 * which is identical to the one produced by a {@link java.io.DataOutput}.
 *
 * <P>Additional features:
 *
 * <ul>
 *
 * <LI>This class provides an internal buffer. By setting a buffer of
 * length 0 at creation time, you can actually bypass the buffering system:
 * Note, however, that several classes providing buffering have synchronised
 * methods, so using a wrapper instead of the internal buffer is likely to lead
 * to a performance drop.
 *
 * <LI>To work around the schizophrenic relationship between streams and random
 * access files in {@link java.io}, this class provides a {@link #flush()}
 * method that byte-aligns the streams, flushes to the underlying byte stream
 * all data and resets the internal state. At this point, you can safely reposition
 * the underlying stream and write again afterwards. For instance, this is safe
 * and will perform as expected:
 * <PRE>
 * FileOutputStream fos = new FileOutputStream(...);
 * ElfOutputBitStream obs = new ElfOutputBitStream(fos);
 * ... write operations on obs ...
 * obs.flush();
 * fos.getChannel().position(...);
 * ... other write operations on obs ...
 * </PRE>
 *
 * <P>As a commodity, an instance of this class will try to cast the underlying
 * byte stream to a {@link RepositionableStream} and to fetch by reflection the
 * {@link FileChannel} underlying the given output stream, in
 * this order.  If either reference can be successfully fetched, you can use
 * directly the {@link #position(long) position()} method with argument
 * <code>pos</code> with the same semantics of a {@link #flush()}, followed by
 * a call to <code>position(pos / 8)</code> (where the latter method belongs
 * either to the underlying stream or to its underlying file channel).  The
 * specified position must be byte aligned, as there is no clean way of reading
 * a fraction of a byte with the current APIs. However, since the reflective checks are quite
 * heavy they can be disabled using a {@linkplain ElfOutputBitStream#ElfOutputBitStream(OutputStream, boolean) suitable constructor}.
 *
 * </ul>
 *
 * <P><STRONG>This class	 is not synchronised</STRONG>. If multiple threads
 * access an instance of this class concurrently, they must be synchronised externally.
 *
 * @see OutputStream
 * @see it.unimi.dsi.io.InputBitStream
 * @author Sebastiano Vigna
 * @since 0.1
 */

public class ElfOutputBitStream implements Flushable, Closeable {

	public static final int MAX_PRECOMPUTED = 4096;

	private final static boolean DEBUG = false;

	/* Precomputed tables: the lower 24 bits contain the (right-aligned) code,
	 * the upper 8 bits contain the code length. */
	public static final int[] GAMMA = new int[MAX_PRECOMPUTED], DELTA = new int[MAX_PRECOMPUTED], ZETA_3 = new int[MAX_PRECOMPUTED],
			SHIFTED_GAMMA = new int[MAX_PRECOMPUTED];

	static {
		/* We load all precomputed arrays from resource files,
		 * to work around the limit on static initialiser code. */
		try {
			ElfInputBitStream.fillArrayFromResource("gamma.out.12", GAMMA);
			ElfInputBitStream.fillArrayFromResource("delta.out.12", DELTA);
			ElfInputBitStream.fillArrayFromResource("zeta3.out.12", ZETA_3);
			ElfInputBitStream.fillArrayFromResource("shiftedgamma.out.12", SHIFTED_GAMMA);
		}
		catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	/** The default size of the byte buffer in bytes (16Ki). */
	public static final int DEFAULT_BUFFER_SIZE = 16 * 1024;
	/** The underlying {@link OutputStream}. */
	protected final OutputStream os;
	/** The number of bits written to this bit stream. */
	private long writtenBits;
	/** Current bit buffer. */
	private int current;
	/** The stream buffer. */
	protected byte[] buffer;
	/** Current number of free bits in the bit buffer (the bits in the buffer are stored high). */
	protected int free;
	/** Current position in the byte buffer. */
	protected int pos;
	/** Current position of the underlying output stream. */
	protected long position;
	/** Current number of bytes available in the byte buffer. */
	protected int avail;
	/** Size of the small buffer for temporary usage. */
	final static int TEMP_BUFFER_SIZE = 128;
	/** The cached file channel underlying {@link #os}. */
	protected final FileChannel fileChannel;
	/** {@link #os} cast to a positionable stream. */
	protected final RepositionableStream repositionableStream;
	/** True if we are wrapping an array. */
	protected final boolean wrapping;


	/** This (non-public) constructor exists just to provide fake initialisation for classes such as {@link DebugOutputBitStream}.
	 */
	protected ElfOutputBitStream() {
		os = null;
		fileChannel = null;
		repositionableStream = null;
		wrapping = false;
	}


	/** Creates a new output bit stream wrapping a given output stream using a buffer of size {@link #DEFAULT_BUFFER_SIZE}.
	 *
	 * <p>This constructor performs the reflective tests that are necessary to support {@link #position(long)}.
	 *
	 * @param os the output stream to wrap.
	 */
	public ElfOutputBitStream(final OutputStream os) {
		this(os, true);
	}

	/** Creates a new output bit stream wrapping a given output stream using a buffer of size {@link #DEFAULT_BUFFER_SIZE}.
	 *
	 * @param os the output stream to wrap.
	 * @param testForPosition if false, the reflective test that is necessary to support {@link #position(long)}
	 * in case <code>os</code> does not support {@link RepositionableStream} will not be performed.
	 */
	public ElfOutputBitStream(final OutputStream os, final boolean testForPosition) {
		this(os, DEFAULT_BUFFER_SIZE);
	}


	/** Creates a new output bit stream wrapping a given output stream with a specified buffer size.
	 *
	 * <p>This constructor performs the reflective tests that are necessary to support {@link #position(long)}.
	 *
	 * @param os the output stream to wrap.
	 * @param bufSize the size in byte of the buffer; it may be 0, denoting no buffering.
	 */
	public ElfOutputBitStream(final OutputStream os, final int bufSize) {
		this(os, bufSize, true);
	}

	/** Creates a new output bit stream wrapping a given output stream with a specified buffer size.
	 *
	 * @param os the output stream to wrap.
	 * @param bufSize the size in byte of the buffer; it may be 0, denoting no buffering.
	 * @param testForPosition if false, the reflective test that is necessary to support {@link #position(long)}
	 * in case <code>os</code> does not support {@link RepositionableStream} will not be performed.
	 */
	public ElfOutputBitStream(final OutputStream os, final int bufSize, final boolean testForPosition) {
		this.os = os;
		wrapping = false;
		if (bufSize != 0) {
			this.buffer = new byte[bufSize];
			avail = bufSize;
		}
		free = 8;

		if (os instanceof RepositionableStream) {
			repositionableStream = (RepositionableStream)os;
			fileChannel = null;
		}
		else if (testForPosition) {
			FileChannel fc = null;
			try {
				fc = (FileChannel)(os.getClass().getMethod("getChannel")).invoke(os, new Object[] {});
			}
			catch(final IllegalAccessException e) {}
			catch(final IllegalArgumentException e) {}
			catch(final NoSuchMethodException e) {}
			catch(final InvocationTargetException e) {}
			catch(final ClassCastException e) {}
			fileChannel = fc;
			repositionableStream = null;
		}
		else {
			repositionableStream = null;
			fileChannel = null;
		}
	}

	/** Creates a new output bit stream wrapping a given file output stream using a buffer of size {@link #DEFAULT_BUFFER_SIZE}.
	 *
	 * <p>This constructor invokes directly {@link FileOutputStream#getChannel()} to support {@link #position(long)}.
	 *
	 * @param os the output stream to wrap.
	 */
	public ElfOutputBitStream(final FileOutputStream os) {
		this(os, DEFAULT_BUFFER_SIZE);
	}

	/** Creates a new output bit stream wrapping a given file output stream with a specified buffer size.
	 *
	 * <p>This constructor invokes directly {@link FileOutputStream#getChannel()} to support {@link #position(long)}.
	 *
	 * @param os the output stream to wrap.
	 * @param bufSize the size in byte of the buffer; it may be 0, denoting no buffering.
	 */
	public ElfOutputBitStream(final FileOutputStream os, final int bufSize) {
		this.os = os;
		wrapping = false;
		if (bufSize != 0) {
			this.buffer = new byte[bufSize];
			avail = bufSize;
		}
		free = 8;
		repositionableStream = null;
		fileChannel = os.getChannel();
	}


	/** Creates a new output bit stream wrapping a given byte array.
	 *
	 * @param a the byte array to wrap.
	 */
	public ElfOutputBitStream(final byte[] a) {
		os = null;
		free = 8;
		buffer = a;
		avail = a.length;
		wrapping = true;
		fileChannel = null;
		repositionableStream = null;
	}

	/** Creates a new output bit stream writing to file.
	 *
	 * @param name the name of the file.
	 * @param bufSize the size in byte of the buffer; it may be 0, denoting no buffering.
	 */
	public ElfOutputBitStream(final String name, final int bufSize) throws FileNotFoundException {
		this(new FileOutputStream(name), bufSize);
	}

	/** Creates a new output bit stream writing to a file.
	 *
	 * @param name the name of the file.
	 */
	public ElfOutputBitStream(final String name) throws FileNotFoundException {
		this(new FileOutputStream(name), DEFAULT_BUFFER_SIZE);
	}


	/** Creates a new output bit stream writing to file.
	 *
	 * @param file the file.
	 * @param bufSize the size in byte of the buffer; it may be 0, denoting no buffering.
	 */
	public ElfOutputBitStream(final File file, final int bufSize) throws FileNotFoundException {
		this(new FileOutputStream(file), bufSize);
	}

	/** Creates a new output bit stream writing to a file.
	 *
	 * @param file the file.
	 */
	public ElfOutputBitStream(final File file) throws FileNotFoundException {
		this(new FileOutputStream(file), DEFAULT_BUFFER_SIZE);
	}


	/** Flushes the bit stream.
	 *
	 * <P>This method will align the stream, write the bit buffer, empty the
	 * byte buffer and delegate to the {@link OutputStream#flush()} method of
	 * the underlying output stream.
	 *
	 * <P>This method is provided so that users of this class can easily wrap
	 * repositionable streams (for instance, file-based streams, which can be
	 * repositioned using the underlying {@link
	 * FileChannel}). <P> It is guaranteed that after calling
	 * this method the underlying stream can be repositioned, and that the next
	 * write to the underlying output stream will start with the content of the
	 * first write method called afterwards.
	 */

	@Override
	public void flush() {
		try {
			align();
			if (os != null) {
				if (buffer != null) {
					os.write(buffer, 0, pos);
					position += pos;
					pos = 0;
					avail = buffer.length;
				}
				os.flush();
			}
		} catch (Exception e) {
		}

	}


	/** Closes the bit stream. All resources associated with the stream are released.
	 */

	@Override
	public void close() throws IOException {
		flush();
		if (os != null && os != System.out && os != System.err) os.close();
		buffer = null;
	}

	/** Returns the number of bits written to this bit stream.
	 *
	 * @return the number of bits written so far.
	 */
	public long writtenBits() {
		return writtenBits;
	}

	/** Sets the number of bits written to this bit stream.
	 *
	 * <P>This method is provided so that, for instance, the
	 * user can reset via <code>writtenBits(0)</code> the written-bits count
	 * after a {@link #flush()}.
	 *
	 * @param writtenBits the new value for the number of bits written so far.
	 */
	public void writtenBits(final long writtenBits) {
		this.writtenBits = writtenBits;
	}

	/** Writes a byte to the stream.
	 *
	 * <P>This method takes care of managing the buffering logic transparently.
	 *
	 * <P>However, this method does <em>not</em> update {@link #writtenBits}.
	 * The caller should increment {@link #writtenBits} by 8 at each call.
	 */

	private void write(final int b) throws IOException {
		if (avail-- == 0) {
			if (buffer == null) {
				os.write(b);
				position++;
				avail = 0;
				return;
			}
			os.write(buffer);
			position += buffer.length;
			avail = buffer.length - 1;
			pos = 0;
		}

		buffer[pos++] = (byte)b;
	}


	/** Writes bits in the bit buffer, possibly flushing it.
	 *
	 * You cannot write more than {@link #free} bits with this method. However,
	 * after having written {@link #free} bits the bit buffer will be empty. In
	 * particular, there should never be 0 free bits in the buffer.
	 *
	 * @param b the bits to write in the <strong>lower</strong> positions; the remaining positions must be zero.
	 * @param len the number of bits to write (0 is safe and causes no action).
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if one tries to write more bits than available in the buffer and debug is enabled.
	 */

	private int writeInCurrent(final int b, final int len) throws IOException {

		current |= (b & ((1 << len) - 1)) << (free -= len);
		if (free == 0) {
			write(current);
			free = 8;
			current = 0;
		}

		writtenBits += len;
		return len;
	}



	/** Aligns the stream.
	 *
	 * After a call to this method, the stream is byte aligned. Zeroes
	 * are used to pad it if necessary.
	 *
	 * @return the number of padding bits.
	 */

	public int align() throws IOException {
		if (free != 8) return writeInCurrent(0, free);
		else return 0;
	}

	/** Sets this stream bit position, if it is based  on a {@link RepositionableStream} or on a {@link FileChannel}.
	 *
	 * <P>Given an underlying stream that implements {@link
	 * RepositionableStream} or that can provide a {@link
	 * FileChannel} via the <code>getChannel()</code> method,
	 * a call to this method has the same semantics of a {@link #flush()},
	 * followed by a call to {@link
	 * FileChannel#position(long) position(position / 8)} on
	 * the byte stream. Currently there is no clean, working way of supporting
	 * out-of-byte-boundary positioning.
	 *
	 * @param position the new position expressed as a bit offset; it must be byte-aligned.
	 * @throws IllegalArgumentException when trying to position outside of byte boundaries.
	 * @throws UnsupportedOperationException if the underlying byte stream does not implement
	 * {@link RepositionableStream} and if the channel it returns is not a {@link FileChannel}.
	 * @see FileChannel#position(long)
	 */

	public void position(final long position) throws IOException {

		if (position < 0) throw new IllegalArgumentException("Illegal position: " + position);
		if ((position & 7) != 0) throw new IllegalArgumentException("Not a byte-aligned position: " + position);

		if (wrapping) {
			if ((position >>> 3) > buffer.length) throw new IllegalArgumentException("Illegal position: " + position);
			flush();
			free = 8;
			pos = (int)(position >>> 3);
			avail = buffer.length - pos;
		}
		else if (repositionableStream != null) {
			flush();
			if (position >>> 3 != this.position) repositionableStream.position(this.position = position >>> 3);
		}
		else if (fileChannel != null) {
			flush();
			if (position >>> 3 != this.position) fileChannel.position(this.position = position >>> 3);
		}
		else throw new UnsupportedOperationException("position() can only be called if the underlying byte stream implements the RepositionableStream interface or if the getChannel() method of the underlying byte stream exists and returns a FileChannel");
	}

	/** Writes a sequence of bits.
	 *
	 * Bits will be written in the natural way: the first bit is bit 7 of the
	 * first byte, the eightth bit is bit 0 of the first byte, the ninth bit is
	 * bit 7 of the second byte and so on.
	 *
	 * @param bits a vector containing the bits to be written.
	 * @param len a bit length.
	 * @return the number of bits written (<code>len</code>).
	 */
	public long write(final byte[] bits, final long len) throws IOException {
		return writeByteOffset(bits, 0, len);
	}


	/** Writes a sequence of bits, starting from a given offset.
	 *
	 * Bits will be written in the natural way: the first bit is bit 7 of the
	 * first byte, the eightth bit is bit 0 of the first byte, the ninth bit is
	 * bit 7 of the second byte and so on.
	 *
	 * @param bits a vector containing the bits to be written.
	 * @param offset a bit offset from which to start to write.
	 * @param len a bit length.
	 * @return the number of bits written (<code>len</code>).
	 */

	public long write(final byte[] bits, final long offset, final long len) throws IOException {
		final int initial = (int)(8 - (offset & 0x7));
		if (initial == 8) return writeByteOffset(bits, (int)offset >>> 3, len);
		if (len <= initial) return writeInt((0xFF & bits[(int)(offset >>> 3)]) >>> (initial - len), (int)len);
		return writeInt(bits[(int)(offset >>> 3)], initial) + writeByteOffset(bits, (int)((offset >>> 3) + 1), len - initial);
	}


	/** Writes a sequence of bits, starting from a given byte offset.
	 *
	 * Bits will be written in the natural way: the first bit is bit 7 of the
	 * first byte, the eightth bit is bit 0 of the first byte, the ninth bit is
	 * bit 7 of the second byte and so on.
	 *
	 * <p>This method is used to support methods such as {@link #write(byte[], long, long)}.
	 *
	 * @param bits a vector containing the bits to be written.
	 * @param offset an offset, expressed in <strong>bytes</strong>.
	 * @param len a bit length.
	 * @return the number of bits written (<code>len</code>).
	 */

	protected long writeByteOffset(final byte[] bits, final int offset, long len) throws IOException {

		if (len == 0) return 0;
		if (len <= free) return writeInCurrent(bits[offset] >>> 8 - len, (int)len);
		else {
			final int shift = free;
			int i, j;

			writeInCurrent(bits[offset] >>> 8 - shift, shift);

			len -= shift;

			j = offset;
			i = (int)(len >> 3);
			while(i-- != 0) {
				write(bits[j] << shift | (bits[j + 1] & 0xFF) >>> 8 - shift);
				writtenBits += 8;
				j++;
			}

			final int queue = (int)(len & 7);
			if (queue != 0) if (queue <= 8 - shift) writeInCurrent(bits[j] >>> 8 - shift - queue, queue);
			else {
				writeInCurrent(bits[j], 8 - shift);
				writeInCurrent(bits[j + 1] >>> 16 - queue - shift, queue + shift - 8);
			}

			return len + shift;
		}

	}


	/** Writes a bit.
	 *
	 * @param bit a bit.
	 * @return the number of bits written.
	 */

	public int writeBit(final boolean bit) {
		try {
			return writeInCurrent(bit ? 1 : 0, 1);
		} catch (IOException e) {
			return -1;
		}
	}

	/** Writes a bit.
	 *
	 * @param bit a bit.
	 * @return the number of bits written.
	 */

	public int writeBit(final int  bit) throws IOException {
		if (bit < 0 || bit > 1) throw new IllegalArgumentException("The argument " + bit + " is not a bit.");
		return writeInCurrent(bit, 1);
	}

	/** Writes a sequence of bits emitted by a boolean iterator.
	 *
	 * <P>If the iterator throws an exception, it is catched,
	 * and the return value is given by the number of bits written
	 * increased by one and with the sign changed.
	 *
	 * @param i a boolean iterator.
	 * @return if <code>i</code> did not throw a runtime exception,
	 * the number of bits written; otherwise, the number of bits written,
	 * plus one, with the sign changed.
	 */

	public int write(final BooleanIterator i) throws IOException {
		int count = 0;
		boolean bit;
		while(i.hasNext()) {
			try {
				bit = i.nextBoolean();
			}
			catch(final RuntimeException hide) {
				return -count - 1;
			}

			writeBit(bit);
			count++;
		}
		return count;
	}


	/** Writes a fixed number of bits from an integer.
	 *
	 * @param x an integer.
	 * @param len a bit length; this many lower bits of the first argument will be written
	 * (the most significant bit first).
	 * @return the number of bits written (<code>len</code>).
	 */

	public int writeInt(int x, final int len) {

		try {
			if (len <= free) return writeInCurrent(x, len);

			int i = len - free;
			final int queue = i & 7;

			if (free != 0) writeInCurrent(x >>> i, free);

			// Dirty trick: since queue < 8, we pre-write the last bits in the bit buffer.
			if (queue != 0) {
				i -= queue;
				writeInCurrent(x, queue);
				x >>>= queue;
			}

			if (i == 32) write(x >>> 24);
			if (i > 23) write(x >>> 16);
			if (i > 15) write(x >>> 8);
			if (i > 7) write(x);

			writtenBits += i;

			return len;
	
		} catch (Exception e) {
			return -1;
		}
	}

	/** Writes a fixed number of bits from a long.
	 *
	 * @param x a long.
	 * @param len a bit length; this many lower bits of the first argument will be written
	 * (the most significant bit first).
	 * @return the number of bits written (<code>len</code>).
	 */

	public int writeLong(long x, final int len) {
		try {
			if (len <= free) return writeInCurrent((int)x, len);

			int i = len - free;
			final int queue = i & 7;

			if (free != 0) writeInCurrent((int)(x >>> i), free);

			// Dirty trick: since queue < 8, we pre-write the last bits in the bit buffer.
			if (queue != 0) {
				i -= queue;
				writeInCurrent((int)x, queue);
				x >>>= queue;
			}

			if (i == 64) write((int)(x >>> 56));
			if (i > 55) write((int)(x >>> 48));
			if (i > 47) write((int)(x >>> 40));
			if (i > 39) write((int)(x >>> 32));
			if (i > 31) write((int)x >>> 24);
			if (i > 23) write((int)x >>> 16);
			if (i > 15) write((int)x >>> 8);
			if (i > 7) write((int)x);

			writtenBits += i;

			return len;
	
		} catch (Exception e) {
			return -1;
		}
		
	}

	/** Writes a natural number in unary coding.
	 *
	 * <p>The unary coding of a natural number <var>n</var> is given
	 * by 0<sup><var>n</var></sup>1.
	 *
	 * @param x a natural number.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number.
	 */

	public int writeUnary(int x) throws IOException {
		if (x < 0) throw new IllegalArgumentException("The argument " + x + " is negative");

		if (x < free) return writeInCurrent(1, x + 1);

		final int shift = free;
		x -= shift;

		writtenBits += shift;
		write(current);
		free = 8;
		current = 0;

		int i = x >> 3;

		writtenBits += (x & 0x7FFFFFF8);

		while(i-- != 0) write(0);

		writeInCurrent(1, (x & 7) + 1);

		return x + shift + 1;
	}

	/** Writes a long natural number in unary coding.
	 *
	 * @param x a long natural number.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number.
	 * @see #writeUnary(int)
	 */

	public long writeLongUnary(long x) throws IOException {
		if (x < 0) throw new IllegalArgumentException("The argument " + x + " is negative");

		if (x < free) return writeInCurrent(1, (int)x + 1);

		final int shift = free;
		x -= shift;

		writtenBits += shift;
		write(current);
		free = 8;
		current = 0;

		long i = x >> 3;

		writtenBits += (x & 0x7FFFFFFFFFFFFFF8L);

		while(i-- != 0) write(0);

		writeInCurrent(1, (int)(x & 7) + 1);

		return x + shift + 1;
	}

	/** Writes a natural number in &gamma; coding.
	 *
	 * <P>The &gamma; coding of a positive number of <var>k</var> bits is
	 * obtained writing <var>k</var>-1 in unary, followed by the lower
	 * <var>k</var>-1 bits of the number. The coding of a natural number is
	 * obtained by adding one and coding.
	 *
	 * @param x a natural number.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number.
	 */

	public int writeGamma(int x) throws IOException {
		if (x < 0) throw new IllegalArgumentException("The argument " + x + " is negative");
		if (x < MAX_PRECOMPUTED) return writeInt(GAMMA[x], GAMMA[x] >>> 26);

		final int msb = Fast.mostSignificantBit(++x);
		return writeUnary(msb) + writeInt(x, msb);
	}

	/** Writes a given amount of natural numbers in &gamma; coding.
	 *
	 * @param a an array at least <code>count</code> natural numbers.
	 * @param count the number of elements of <code>a</code> to be written.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number.
	 * @see #writeGamma(int)
	 */

	public long writeGammas(final int[] a, final int count) throws IOException {
		long l = 0;
		for(int i = 0; i < count; i++) {
			int x = a[i];
			if (x < MAX_PRECOMPUTED) {
				l += writeInt(GAMMA[x], GAMMA[x] >>> 26);
				continue;
			}

			final int msb = Fast.mostSignificantBit(++x);
			l += writeUnary(msb) + writeInt(x, msb);
		}
		return l;
	}

	/** Writes a long natural number in &gamma; coding.
	 *
	 * @param x a long natural number.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number.
	 * @see #writeGamma(int)
	 */

	public int writeLongGamma(long x) throws IOException {
		if (x < 0) throw new IllegalArgumentException("The argument " + x + " is negative");
		if (x < MAX_PRECOMPUTED) return writeInt(GAMMA[(int)x], GAMMA[(int)x] >>> 26);

		final int msb = Fast.mostSignificantBit(++x);
		return writeUnary(msb) + writeLong(x, msb);
	}

	/** Writes a natural number in shifted &gamma; coding.
	 *
	 * The shifted &gamma; coding of 0 is 1. The coding of a positive number
	 * of <var>k</var> bits is
	 * obtained writing <var>k</var> in unary, followed by the lower
	 * <var>k</var>-1 bits of the number (equivalently, by writing
	 * <var>k</var> zeroes followed by the number).
	 *
	 * @param x a natural number.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number.
	 */

	public int writeShiftedGamma(final int x) throws IOException {
		if (x < 0) throw new IllegalArgumentException("The argument " + x + " is negative");
		if (x < MAX_PRECOMPUTED) return writeInt(SHIFTED_GAMMA[x], SHIFTED_GAMMA[x] >>> 26);

		final int msb = Fast.mostSignificantBit(x);
		return writeUnary(msb + 1) + (msb > 0 ? writeInt(x, msb) : 0);
	}

	/** Writes a long natural number in shifted &gamma; coding.
	 *
	 * @param x a natural number.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number.
	 * @see #writeShiftedGamma(int)
	 */

	public int writeLongShiftedGamma(final long x) throws IOException {
		if (x < 0) throw new IllegalArgumentException("The argument " + x + " is negative");
		if (x < MAX_PRECOMPUTED) return writeInt(SHIFTED_GAMMA[(int)x], SHIFTED_GAMMA[(int)x] >>> 26);

		final int msb = Fast.mostSignificantBit(x);
		return writeUnary(msb + 1) + (msb > 0 ? writeLong(x, msb) : 0);
	}

	/** Writes a given amount of natural numbers in shifted &gamma; coding.
	 *
	 * @param a an array at least <code>count</code> natural numbers.
	 * @param count the number of elements of <code>a</code> to be written.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number.
	 * @see #writeShiftedGamma(int)
	 */

	public long writeShiftedGammas(final int[] a, final int count) throws IOException {
		long l = 0;
		for(int i = 0; i < count; i++) {
			final int x = a[i];
			if (x < MAX_PRECOMPUTED) {
				l += writeInt(SHIFTED_GAMMA[x], SHIFTED_GAMMA[x] >>> 26);
				continue;
			}

			final int msb = Fast.mostSignificantBit(x);
			l += writeUnary(msb + 1) + (msb > 0 ? writeInt(x, msb) : 0);
		}
		return l;
	}

	/** Writes a natural number in &delta; coding.
	 *
	 * The &delta; coding of a positive number of <var>k</var> bits is
	 * obtained writing <var>k</var>-1 in &gamma; coding, followed by the
	 * lower <var>k</var>-1 bits of the number. The coding of a natural
	 * number is obtained by adding one and coding.
	 *
	 * @param x a natural number.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number.
	 */

	public int writeDelta(int x) throws IOException {
		if (x < 0) throw new IllegalArgumentException("The argument " + x + " is negative");
		if (x < MAX_PRECOMPUTED) return writeInt(DELTA[x], DELTA[x] >>> 26);

		final int msb = Fast.mostSignificantBit(++x);
		return writeGamma(msb) + writeInt(x, msb);
	}

	/** Writes a long natural number in &delta; coding.
	 *
	 * @param x a long natural number.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number.
	 * @see #writeDelta(int)
	 */

	public int writeLongDelta(long x) throws IOException {
		if (x < 0) throw new IllegalArgumentException("The argument " + x + " is negative");
		if (x < MAX_PRECOMPUTED) return writeInt(DELTA[(int)x], DELTA[(int)x] >>> 26);

		final int msb = Fast.mostSignificantBit(++x);
		return writeGamma(msb) + writeLong(x, msb);
	}

	/** Writes a given amount of natural numbers in &delta; coding.
	 *
	 * @param a an array at least <code>count</code> natural numbers.
	 * @param count the number of elements of <code>a</code> to be written.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number.
	 * @see #writeDelta(int)
	 */

	public long writeDeltas(final int[] a, final int count) throws IOException {
		long l = 0;
		for(int i = 0; i < count; i++) {
			int x = a[i];
			if (x < MAX_PRECOMPUTED) {
				l += writeInt(DELTA[x], DELTA[x] >>> 26);
				continue;
			}

			final int msb = Fast.mostSignificantBit(++x);
			l += writeGamma(msb) + writeInt(x, msb);
		}
		return l;
	}

	/** Writes a natural number in a limited range using a minimal binary coding.
	 *
	 * <p>A minimal binary code is an optimal code for the uniform distribution.
	 * This method uses an optimal code in which shorter words are assigned to
	 * smaller integers.
	 *
	 * @param x a natural number.
	 * @param b a strict upper bound for <code>x</code>.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number or use a nonpositive base.
	 */

	public int writeMinimalBinary(final int x, final int b) throws IOException {
		if (b < 1) throw new IllegalArgumentException("The bound " + b + " is not positive");

		return writeMinimalBinary(x, b, Fast.mostSignificantBit(b));
	}

	/** Writes a natural number in a limited range using a minimal binary coding.
	 *
	 * This method is faster than {@link #writeMinimalBinary(int,int)} because it does not
	 * have to compute <code>log2b</code>.
	 *
	 * @param x a natural number.
	 * @param b a strict upper bound for <code>x</code>.
	 * @param log2b the floor of the base-2 logarithm of the bound.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number or use a nonpositive base.
	 * @see #writeMinimalBinary(int, int)
	 */

	public int writeMinimalBinary(final int x, final int b, final int log2b) throws IOException {
		if (x < 0) throw new IllegalArgumentException("The argument " + x + " is negative");
		if (b < 1) throw new IllegalArgumentException("The bound " + b + " is not positive");
		if (x >= b) throw new IllegalArgumentException("The argument " + x + " exceeds the bound " + b);

		// Numbers smaller than m are encoded in log2b bits.
		final int m = (1 << log2b + 1) - b;

		if (x < m) return writeInt(x, log2b);
		else return writeInt(m + x, log2b + 1);
	}


	/** Writes a long natural number in a limited range using a minimal binary coding.
	 *
	 * @param x a natural number.
	 * @param b a strict upper bound for <code>x</code>.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number or use a nonpositive base.
	 * @see #writeMinimalBinary(int, int)
	 */

	public int writeLongMinimalBinary(final long x, final long b) throws IOException {
		if (b < 1) throw new IllegalArgumentException("The bound " + b + " is not positive");

		return writeLongMinimalBinary(x, b, Fast.mostSignificantBit(b));
	}


	/** Writes a long natural number in a limited range using a minimal binary coding.
	 *
	 * This method is faster than {@link #writeLongMinimalBinary(long,long)} because it does not
	 * have to compute <code>log2b</code>.
	 *
	 * @param x a long natural number.
	 * @param b a strict upper bound for <code>x</code>.
	 * @param log2b the floor of the base-2 logarithm of the bound.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number or use a nonpositive base.
	 * @see #writeMinimalBinary(int, int)
	 */

	public int writeLongMinimalBinary(final long x, final long b, final int log2b) throws IOException {
		if (x < 0) throw new IllegalArgumentException("The argument " + x + " is negative");
		if (b < 1) throw new IllegalArgumentException("The bound " + b + " is not positive");
		if (x >= b) throw new IllegalArgumentException("The argument " + x + " exceeds the bound " + b);

		// Numbers smaller than m are encoded in log2b bits.
		final long m = (1L << log2b + 1) - b;

		if (x < m) return writeLong(x, log2b);
		else return writeLong(m + x, log2b + 1);
	}



	/** Writes a natural number in Golomb coding.
	 *
	 * <p>Golomb coding with modulo <var>b</var> writes a natural number <var>x</var> as the quotient of
	 * the division of <var>x</var> and <var>b</var> in {@linkplain #writeUnary(int) unary},
	 * followed by the remainder in {@linkplain #writeMinimalBinary(int, int) minimal binary code}.
	 *
	 * <P>This method implements also the case in which <code>b</code> is 0: in this case,
	 * the argument <code>x</code> may only be zero, and nothing will be written.
	 *
	 * @param x a natural number.
	 * @param b the modulus for the coding.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number or use a negative modulus.
	 */

	public int writeGolomb(final int x, final int b) throws IOException {
		return writeGolomb(x, b, Fast.mostSignificantBit(b));
	}

	/** Writes a natural number in Golomb coding.
	 *
	 * This method is faster than {@link #writeGolomb(int,int)} because it does not
	 * have to compute <code>log2b</code>.
	 *
	 * @param x a natural number.
	 * @param b the modulus for the coding.
	 * @param log2b the floor of the base-2 logarithm of the coding modulus (it is irrelevant when <code>b</code> is zero).
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number or use a negative modulus.
	 * @see #writeGolomb(int, int)
	 */

	public int writeGolomb(final int x, final int b, final int log2b) throws IOException {
		if (x < 0) throw new IllegalArgumentException("The argument " + x + " is negative");
		if (b < 0) throw new IllegalArgumentException("The modulus " + b + " is negative");
		if (b == 0) {
			if (x != 0) throw new IllegalArgumentException("The modulus is 0, but the argument is " + x);
			return 0;
		}

		final int l = writeUnary(x / b);

		// The remainder to be encoded.
		return l + writeMinimalBinary(x % b, b, log2b);
	}



	/** Writes a long natural number in Golomb coding.
	 *
	 * @param x a long natural number.
	 * @param b the modulus for the coding.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number or use a negative modulus.
	 * @see #writeGolomb(int, int)
	 */

	public long writeLongGolomb(final long x, final long b) throws IOException {
		return writeLongGolomb(x, b, Fast.mostSignificantBit(b));
	}

	/** Writes a long natural number in Golomb coding.
	 *
	 * This method is faster than {@link #writeLongGolomb(long,long)} because it does not
	 * have to compute <code>log2b</code>.
	 *
	 * @param x a long natural number.
	 * @param b the modulus for the coding.
	 * @param log2b the floor of the base-2 logarithm of the coding modulus (it is irrelevant when <code>b</code> is zero).
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number or use a negative modulus.
	 * @see #writeGolomb(int, int)
	 */

	public long writeLongGolomb(final long x, final long b, final int log2b) throws IOException {

		if (x < 0) throw new IllegalArgumentException("The argument " + x + " is negative");
		if (b < 0) throw new IllegalArgumentException("The modulus " + b + " is negative");
		if (b == 0) {
			if (x != 0) throw new IllegalArgumentException("The modulus is 0, but the argument is " + x);
			return 0;
		}

		final long l = writeLongUnary(x / b);

		// The remainder to be encoded.
		return l + writeLongMinimalBinary(x % b, b, log2b);
	}


	/** Writes a natural number in skewed Golomb coding.
	 *
	 * <P>This method implements also the case in which <code>b</code> is 0: in this case,
	 * the argument <code>x</code> may only be zero, and nothing will be written.
	 *
	 * @param x a natural number.
	 * @param b the modulus for the coding.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number or use a negative modulus.
	 */

	public int writeSkewedGolomb(final int x, final int b) throws IOException {
		if (x < 0) throw new IllegalArgumentException("The argument " + x + " is negative");
		if (b < 0) throw new IllegalArgumentException("The modulus " + b + " is negative");
		if (b == 0) {
			if (x != 0) throw new IllegalArgumentException("The modulus is 0, but the argument is " + x);
			return 0;
		}

		final int i = Fast.mostSignificantBit(x / b + 1);
		final int l = writeUnary(i);
		final int M = ((1 << i + 1) - 1) * b;
		final int m = (M / (2 * b)) * b;

		return l + writeMinimalBinary(x - m, M - m);
	}

	/** Writes a long natural number in skewed Golomb coding.
	 *
	 * <P>This method implements also the case in which <code>b</code> is 0: in this case,
	 * the argument <code>x</code> may only be zero, and nothing will be written.
	 *
	 * @param x a long natural number.
	 * @param b the modulus for the coding.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number or use a negative modulus.
	 * @see #writeSkewedGolomb(int, int)
	 */

	public long writeLongSkewedGolomb(final long x, final long b) throws IOException {
		if (x < 0) throw new IllegalArgumentException("The argument " + x + " is negative");
		if (b < 0) throw new IllegalArgumentException("The modulus " + b + " is negative");
		if (b == 0) {
			if (x != 0) throw new IllegalArgumentException("The modulus is 0, but the argument is " + x);
			return 0;
		}

		final long i = Fast.mostSignificantBit(x / b + 1);
		final long l = writeLongUnary(i);
		final long M = ((1L << i + 1) - 1) * b;
		final long m = (M / (2 * b)) * b;

		return l + writeLongMinimalBinary(x - m, M - m);
	}


	/** Writes a natural number in &zeta; coding.
	 *
	 * <P>&zeta; coding (with modulo <var>k</var>) records positive numbers in
	 * the intervals
	 * [1,2<sup><var>k</var></sup>-1],[2<sup><var>k</var></sup>,2<sup><var>k</var>+1</sup>-1],&hellip;,[2<sup><var>hk</var></sup>,2<sup>(<var>h</var>+1)<var>k</var></sup>-1]
	 * by coding <var>h</var> in unary, followed by a minimal binary coding of
	 * the offset in the interval.  The coding of a natural number is obtained
	 * by adding one and coding.
	 *
	 * <P>&zeta; codes were defined by
	 * Paolo Boldi and Sebastiano Vigna in
	 * &ldquo;<a href="http://vigna.di.unimi.it/papers.php#BoVCWWW">Codes for the World&minus;Wide Web</a>&rdquo;,
	 * <i>Internet Math.</i>, 2(4):405-427, 2005. The paper contains also a detailed analysis.
	 *
	 * @param x a natural number.
	 * @param k the shrinking factor.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number or use a nonpositive shrinking factor.
	 */

	public int writeZeta(int x, final int k) throws IOException {
		if (x < 0) throw new IllegalArgumentException("The argument " + x + " is negative");
		if (k < 1) throw new IllegalArgumentException("The shrinking factor " + k + " is not positive");
		if (k == 3 && x < MAX_PRECOMPUTED) return writeInt(ZETA_3[x], ZETA_3[x] >>> 26);

		final int msb = Fast.mostSignificantBit(++x);
		final int h = msb / k;
		final int l = writeUnary(h);
		final int left = 1 << h * k;
		return l + (x - left < left
				? writeInt(x - left, h * k + k - 1)
						: writeInt(x, h * k + k));
	}

	/** Writes a long natural number in &zeta; coding.
	 *
	 * @param x a long natural number.
	 * @param k the shrinking factor.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number or use a nonpositive shrinking factor.
	 * @see #writeZeta(int, int)
	 */

	public int writeLongZeta(long x, final int k) throws IOException {
		if (x < 0) throw new IllegalArgumentException("The argument " + x + " is negative");
		if (k < 1) throw new IllegalArgumentException("The shrinking factor " + k + " is not positive");
		if (k == 3 && x < MAX_PRECOMPUTED) return writeInt(ZETA_3[(int)x], ZETA_3[(int)x] >>> 26);

		final int msb = Fast.mostSignificantBit(++x);
		final int h = msb / k;
		final int l = writeUnary(h);
		final long left = 1L << h * k;
		return l + (x - left < left
				? writeLong(x - left, h * k + k - 1)
						: writeLong(x, h * k + k));
	}


	/** Writes a natural number in variable-length nibble coding.
	 *
	 * <P>Variable-length nibble coding records a natural number by padding its binary
	 * representation to the left using zeroes, until its length is a multiple of three.
	 * Then, the resulting string is
	 * broken in blocks of 3 bits, and each block is prefixed with a bit, which is
	 * zero for all blocks except for the last one.
	 * @param x a natural number.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number.
	 */

	public int writeNibble(final int x) throws IOException {
		if (x < 0) throw new IllegalArgumentException("The argument " + x + " is negative");

		if (x == 0) return writeInt(8, 4);
		final int msb = Fast.mostSignificantBit(x);
		int h = msb / 3;
		do {
			writeBit(h == 0);
			writeInt(x >> h * 3 , 3);
		} while(h-- != 0);
		return ((msb / 3) + 1) << 2;
	}

	/** Writes a long natural number in variable-length nibble coding.
	 *
	 * @param x a long natural number.
	 * @return the number of bits written.
	 * @throws IllegalArgumentException if you try to write a negative number.
	 * @see #writeNibble(int)
	 */

	public int writeLongNibble(final long x) throws IOException {
		if (x < 0) throw new IllegalArgumentException("The argument " + x + " is negative");

		if (x == 0) return writeInt(8, 4);
		final int msb = Fast.mostSignificantBit(x);
		int h = msb / 3;
		do {
			writeBit(h == 0);
			writeInt((int)(x >> h * 3) , 3);
		} while(h-- != 0);
		return ((msb / 3) + 1) << 2;
	}

	/** Copies a given number of bits from a given input bit stream into this output bit stream.
	 *
	 * @param ibs an input bit stream.
	 * @param length the number of bits to copy.
	 * @throws EOFException if there are not enough bits to copy.
	 */
	public void copyFrom(final ElfInputBitStream ibs, long length) throws IOException {
		final byte[] buffer = new byte[64 * 1024];
		while(length > 0) {
			final int toRead = (int)Math.min(length, buffer.length * Byte.SIZE);
			ibs.read(buffer, toRead);
			write(buffer, 0, toRead);
			length -= toRead;
		}
	}

	public byte[] getBuffer() {
		return buffer;
	}

	/** Bytes written to the wrapped array after {@link #flush()}. */
	public int getByteLength() {
		return pos;
	}
}
