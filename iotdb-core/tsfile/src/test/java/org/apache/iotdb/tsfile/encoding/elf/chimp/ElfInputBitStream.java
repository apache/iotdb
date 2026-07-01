package org.apache.iotdb.tsfile.encoding.elf.chimp;

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.booleans.BooleanIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.RepositionableStream;
import it.unimi.dsi.io.DebugInputBitStream;
import it.unimi.dsi.io.NullInputStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.FileChannel;


/** Bit-level input stream.
 *
 * <P>This class wraps any {@link InputStream} so that you can treat it as
 * <em>bit</em> stream.  Constructors and methods closely resemble those of
 * {@link InputStream}. Data can be read from such a stream in several ways:
 * reading a (long) natural number in fixed-width, unary, &gamma;, shifted &gamma;, &delta;, &zeta; and (skewed)
 * Golomb coding, or reading a number of bits that will be stored in a vector of
 * bytes. There is limited support for {@link #mark(int)}/{@link #reset()}
 * operations.
 *
 * <P>This class can also {@linkplain #ElfInputBitStream(byte[]) wrap a byte
 * array}; this is much more lightweight than wrapping a {@link
 * it.unimi.dsi.fastutil.io.FastByteArrayInputStream} wrapping the array. Overflowing the array
 * will cause an {@link EOFException}.
 *
 * <P>Note that when reading using a vector of bytes bits are read in the
 * stream format (see {@link OutputBitStream}): the first bit is bit 7 of the
 * first byte, the eighth bit is bit 0 of the first byte, the ninth bit is bit
 * 7 of the second byte and so on. When reading natural numbers using some coding,
 * instead, they are stored in the standard way, that is, in the <strong>lower</strong>
 * bits.
 *
 * <P>Additional features:
 *
 * <UL>
 *
 * <LI>This class provides an internal buffer. By setting a buffer of
 * length 0 at creation time, you can actually bypass the buffering system:
 * Note, however, that several classes providing buffering have synchronised
 * methods, so using a wrapper instead of the internal buffer is likely to lead
 * to a performance drop.
 *
 * <LI>To work around the schizophrenic relationship between streams and random
 * access files in {@link java.io}, this class provides a {@link #flush()}
 * method that resets the internal state. At this point, you can safely reposition
 * the underlying stream and read again afterwards. For instance, this is safe
 * and will perform as expected:
 * <PRE>
 * FileInputStream fis = new FileInputStream(...);
 * ElfInputBitStream ibs = new ElfInputBitStream(fis);
 * ... read operations on ibs ...
 * ibs.flush();
 * fis.getChannel().position(...);
 * ... other read operations on ibs ...
 * </PRE>
 *
 * <P>As a commodity, an instance of this class will try to cast the underlying byte
 * stream to a {@link RepositionableStream} and to fetch by reflection the {@link
 * FileChannel} underlying the given input stream, in this
 * order.  If either reference can be successfully fetched, you can use
 * directly the {@link #position(long) position()} method with argument
 * <code>pos</code> with the same semantics of a {@link #flush()}, followed by
 * a call to <code>position(pos / 8)</code> (where the latter method belongs
 * either to the underlying stream or to its underlying file channel), followed
 * by a {@link #skip(long) skip(pos % 8)}. However, since the reflective checks are quite
 * heavy they can be disabled using a {@linkplain ElfInputBitStream#ElfInputBitStream(InputStream, boolean) suitable constructor}.
 *
 * <li>Finally, this class implements partially the interface of a boolean iterator.
 * More precisely, {@link #nextBoolean()} will return the same bit as {@link #readBit()},
 * and also the same exceptions, whereas <em>{@link #hasNext()} will always return true</em>:
 * you must be prepared to catch a {@link RuntimeException} wrapping an {@link IOException}
 * in case the file ends. It
 * is very difficult to implement completely an eager operator using a input-stream
 * based model.
 *
 * </ul>
 *
 * <P><STRONG>This class is not synchronised</STRONG>. If multiple threads
 * access an instance of this class concurrently, they must be synchronised externally.
 *
 * @see InputStream
 * @see it.unimi.dsi.io.OutputBitStream
 * @author Sebastiano Vigna
 * @since 0.1
 */

public class ElfInputBitStream implements BooleanIterator, Flushable, Closeable {
	private final static boolean DEBUG = false;

	/* Precomputed tables: the i-th entry decodes the stream fragment of 16 bits given by the binary reprentation of i.
	 * The upper 16 bits contain code lengths, the lower 16 bits decoded values. 0 means undecodable. */
	public static final int[] GAMMA = new int[256 * 256], DELTA = new int[256 * 256], ZETA_3 = new int[256 * 256], SHIFTED_GAMMA = new int[256 * 256];

	static void fillArrayFromResource(final String resource, final int array[]) throws IOException {
		final String resouceFullPath = "/it/unimi/dsi/io/" + resource;
        final InputStream ris = ElfInputBitStream.class.getResourceAsStream(resouceFullPath);
        if (ris == null) throw new IOException("Cannot open resource " + resouceFullPath);
        final DataInputStream dis = new DataInputStream(new FastBufferedInputStream(ris));
		BinIO.loadInts(dis, array, 0, array.length);
		dis.close();
		assert checkLength(resource, array, resouceFullPath);
	}

	public static boolean checkLength(final String resource, final int[] array, final String resouceFullPath) {
		final DataInputStream dis = new DataInputStream(ElfInputBitStream.class.getResourceAsStream(resouceFullPath));
		final int actualLength = IntIterators.unwrap(BinIO.asIntIterator(dis)).length;
		assert array.length == actualLength : resource + " is long " + actualLength + " but we think it should rather be " + array.length;
		try {
			dis.close();
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
		return true;
	}

	static {
		/* We load all precomputed arrays from resource files,
		 * to work around the limit on static initialiser code. */
		try {
			fillArrayFromResource("gamma.in.16", GAMMA);
			fillArrayFromResource("delta.in.16", DELTA);
			fillArrayFromResource("zeta3.in.16", ZETA_3);
			fillArrayFromResource("shiftedgamma.in.16", SHIFTED_GAMMA);
		}
		catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	/** The default size of the byte buffer in bytes (8Ki). */
	public static final int DEFAULT_BUFFER_SIZE = 8 * 1024;
	/** The underlying {@link InputStream}. */
	protected final InputStream is;
	/** Whether we should use the byte buffer. */
//	private final boolean noBuffer;
	/** The cached file channel underlying {@link #is}, if any. */
	protected final FileChannel fileChannel;
	/** {@link #is} cast to a positionable stream, if possible. */
	protected final RepositionableStream repositionableStream;
	/** True if we are wrapping an array. */
	protected final boolean wrapping;
	/** The number of bits actually read from this bit stream. */
	private long readBits;
	/** Current bit buffer: the lowest {@link #fill} bits represent the current content (the remaining bits are undefined). */
	private int current;
	/** The stream buffer. */
	protected byte[] buffer;
	/** Current number of bits in the bit buffer (stored low). */
	protected int fill;
	/** Current position in the byte buffer. */
	protected int pos;
	/** Current number of bytes available in the byte buffer. */
	protected int avail;
	/** Current position of the first byte in the byte buffer. */
	protected long position;


	/** This (non-public) constructor exists just to provide fake initialisation for classes such as {@link DebugInputBitStream}.
	 */
	protected ElfInputBitStream() {
		is = null;
//		noBuffer = true;
		repositionableStream = null;
		fileChannel = null;
		wrapping = false;
	}

	/** Creates a new input bit stream wrapping a given input stream using a buffer of size {@link #DEFAULT_BUFFER_SIZE}.
	 *
	 * <p>This constructor performs the reflective tests that are necessary to support {@link #position(long)}.
	 *
	 * @param is the input stream to wrap.
	 */
	public ElfInputBitStream(final InputStream is) {
		this(is, true);
	}


	/** Creates a new input bit stream wrapping a given input stream using a buffer of size {@link #DEFAULT_BUFFER_SIZE}.
	 *
	 * @param is the input stream to wrap.
	 * @param testForPosition if false, the reflective test that is necessary to support {@link #position(long)}
	 * in case <code>is</code> does not implement {@link RepositionableStream} will not be performed.
	 */
	public ElfInputBitStream(final InputStream is, final boolean testForPosition) {
		this(is, DEFAULT_BUFFER_SIZE, testForPosition);
	}

	/** Creates a new input bit stream wrapping a given input stream with a specified buffer size.
	 *
	 * <p>This constructor performs the reflective tests that are necessary to support {@link #position(long)}.
	 *
	 * @param is the input stream to wrap.
	 * @param bufSize the size in byte of the buffer; it may be 0, denoting no buffering.
	 */
	public ElfInputBitStream(final InputStream is, final int bufSize) {
		this(is, bufSize, true);
	}

	/** Creates a new input bit stream wrapping a given input stream with a specified buffer size.
	 *
	 * @param is the input stream to wrap.
	 * @param bufSize the size in byte of the buffer; it may be 0, denoting no buffering.
	 * @param testForPosition if false, the reflective test that is necessary to support {@link #position(long)}
	 * in case <code>is</code> does not implement {@link RepositionableStream} will not be performed.
	 */
	public ElfInputBitStream(final InputStream is, final int bufSize, final boolean testForPosition) {
		this.is = is;
		wrapping = false;
		this.buffer = new byte[bufSize];

		// Cheap test, we do it all the time
		if (is instanceof RepositionableStream) {
			repositionableStream = (RepositionableStream)is;
			fileChannel = null;
		}
		else  if (testForPosition) {
			FileChannel fc = null;
			try {
				fc = (FileChannel)(is.getClass().getMethod("getChannel")).invoke(is);
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

	/** Creates a new input bit stream wrapping a given file input stream using a buffer of size {@link #DEFAULT_BUFFER_SIZE}.
	 *
	 * <p>This constructor invokes directly {@link FileInputStream#getChannel()} to support {@link #position(long)}.
	 *
	 * @param is the file input stream to wrap.
	 */
	public ElfInputBitStream(final FileInputStream is) {
		this(is, DEFAULT_BUFFER_SIZE);
	}

	/** Creates a new input bit stream wrapping a given file input stream with a specified buffer size.
	 *
	 * <p>This constructor invokes directly {@link FileInputStream#getChannel()} to support {@link #position(long)}.
	 *
	 * @param is the file input stream to wrap.
	 * @param bufSize the size in byte of the buffer; it may be 0, denoting no buffering.
	 */
	public ElfInputBitStream(final FileInputStream is, final int bufSize) {
		this.is = is;
		wrapping = false;
		this.buffer = new byte[bufSize];
		repositionableStream = null;
		fileChannel = is.getChannel();
	}

	/** Creates a new input bit stream wrapping a given byte array.
	 *
	 * @param a the byte array to wrap.
	 */
	public ElfInputBitStream(final byte[] a) {
		is = NullInputStream.getInstance();
		repositionableStream = null;
		fileChannel = null;

//		if (a.length > 0) {
		buffer = a;
		avail = a.length;
		wrapping = true;
//			noBuffer = false;
//		}
//		else {
//			// A zero-length buffer is like having no buffer
//			buffer = null;
//			avail = 0;
//			wrapping = false;
//			noBuffer = true;
//		}
	}

	/** Creates a new input bit stream reading from a file.
	 *
	 * <p>This constructor invokes directly {@link FileInputStream#getChannel()} to support {@link #position(long)}.
	 *
	 * @param name the name of the file.
	 * @param bufSize the size in byte of the buffer; it may be 0, denoting no buffering.
	 */
	public ElfInputBitStream(final String name, final int bufSize) throws FileNotFoundException {
		this(new FileInputStream(name), bufSize);
	}

	/** Creates a new input bit stream reading from a file.
	 *
	 * <p>This constructor invokes directly {@link FileInputStream#getChannel()} to support {@link #position(long)}.
	 *
	 * @param name the name of the file.
	 */
	public ElfInputBitStream(final String name) throws FileNotFoundException {
		this(new FileInputStream(name), DEFAULT_BUFFER_SIZE);
	}

	/** Creates a new input bit stream reading from a file.
	 *
	 * <p>This constructor invokes directly {@link FileInputStream#getChannel()} to support {@link #position(long)}.
	 *
	 * @param file the file.
	 */
	public ElfInputBitStream(final File file) throws FileNotFoundException {
		this(new FileInputStream(file), DEFAULT_BUFFER_SIZE);
	}

	/** Creates a new input bit stream reading from a file.
	 *
	 * <p>This constructor invokes directly {@link FileInputStream#getChannel()} to support {@link #position(long)}.
	 *
	 * @param file the file.
	 * @param bufSize the size in byte of the buffer; it may be 0, denoting no buffering.
	 */
	public ElfInputBitStream(final File file, final int bufSize) throws FileNotFoundException {
		this(new FileInputStream(file), bufSize);
	}


	/** Flushes the bit stream. All state information associated with the stream is reset. This
	 * includes bytes prefetched from the stream, bits in the bit buffer and unget'd bits.
	 *
	 * <P>This method is provided so that users of this class can easily wrap repositionable
	 * streams (for instance, file-based streams, which can be repositioned using
	 * the underlying {@link FileChannel}). It is guaranteed that after calling
	 * this method the underlying stream can be repositioned, and that the next read
	 * will draw data from the stream.
	 */

	@Override
	public void flush() {
		if (! wrapping) {
			position += pos;
			avail = 0;
			pos = 0;
		}
		fill = 0;
	}

	/** Closes the bit stream. All resources associated with the stream are released.
	 */

	@Override
	public void close() throws IOException {
		if (is != null && is != System.in) is.close();
		buffer = null;
	}

	/** Returns the number of bits that can be read (or skipped over) from this
	 * bit stream without blocking by the next caller of a method.
	 *
	 * @return the number of bits that can be read from this bit stream without blocking.
	 */

	public long available() throws IOException {
		return (is.available() + avail) * 8 + fill;
	}


	/** Returns the number of bits read from this bit stream.
	 *
	 * @return the number of bits read so far.
	 */
	public long readBits() {
		return readBits;
	}

	/** Sets the number of bits read from this bit stream.
	 *
	 * <P>This method is provided so that, for instance, the
	 * user can reset via <code>readBits(0)</code> the read-bits count
	 * after a {@link #flush()}.
	 *
	 * @param readBits the new value for the number of bits read so far.
	 */
	public void readBits(final long readBits) {
		this.readBits = readBits;
	}

	/** Reads the next byte from the stream.
	 *
	 * <P>This method takes care of managing the buffering logic
	 * transparently.
	 *
	 * <P>However, this method does <em>not</em> update {@link #readBits}.
	 * The caller should increment {@link #readBits} by 8 at each call, unless
	 * the bit are used to load {@link #current}.
	 */

	private final int read() throws IOException {
//		if (noBuffer) {
//			final int t = is.read();
//			position++;
//			return t;
//		}

		if (avail == 0) {
			avail = is.read(buffer);
			position += pos;
			pos = 0;
		}

		avail--;
		return buffer[pos++] & 0xFF;
	}

	/** Feeds 16 more bits into {@link #current}, assuming that {@link #fill} is less than 16.
	 *
	 * <p>This method will never throw an {@link EOFException}&mdash;simply, it will refill less than 16 bits.
	 *
	 * @return {@link #fill}.
	 */

	private final int refill() throws IOException {

//		current = current << 16 | (buffer[pos++] & 0xFF) << 8 | buffer[pos++] & 0xFF;
//		return fill += 16;

		if (avail > 1) {
			// If there is a byte in the buffer, we use it directly.
			avail -= 2;
			current = current << 16 | (buffer[pos++] & 0xFF) << 8 | buffer[pos++] & 0xFF;
			return fill += 16;
		} else {
		}

		current = (current << 8) | read();
		fill += 8;
		current = (current << 8) | read();
		fill += 8;

		return fill;
	}


	/** Reads bits from the bit buffer, possibly refilling it.
	 *
	 * <P>This method is the basic mean for extracting bits from the underlying stream.
	 *
	 * <P>You cannot read more than {@link #fill} bits with this method (unless {@link #fill} is 0,
	 * and <code>len</code> is nonzero, in which case the buffer will be refilled for you with 8 bits), and if you
	 * read exactly {@link #fill} bits the buffer will be empty afterwards. In particular,
	 * there will never be 8 bits in the buffer.
	 *
	 * <P>The bit buffer stores its content in the lower {@link #fill} bits. The content
	 * of the remaining bits is undefined.
	 *
	 * <P>This method updates {@link #readBits}.
	 *
	 * @param len the number of bits to read.
	 * @return the bits read (in the <strong>lower</strong> positions).
	 * @throws AssertionError if one tries to read more bits than available in the buffer and assertions are enabled.
	 */

	private final int readFromCurrent(final int len) throws IOException {

		if (fill == 0) {
			current = read();
			fill = 8;
		}

		return current >>> (fill -= len) & (1 << len) - 1;
	}

	/** Aligns the stream.
	 *
	 * After a call to this function, the stream is byte aligned. Bits that have been
	 * read to align are discarded.
	 */

	public void align() {
		if ((fill & 7) == 0) return;
		readBits += fill & 7;
		fill &= ~7;
	}



	/** Reads a sequence of bits.
	 *
	 * Bits will be read in the natural way: the first bit is bit 7 of the
	 * first byte, the eightth bit is bit 0 of the first byte, the ninth bit is
	 * bit 7 of the second byte and so on.
	 *
	 * @param bits an array of bytes to store the result.
	 * @param len the number of bits to read.
	 */

	public void read(final byte[] bits, int len) throws IOException {
		assert fill < 32 : fill + " >= " + 32;

		if (len <= fill) {
			if (len <= 8) {
				bits[0] = (byte)(readFromCurrent(len) << 8 - len);
				return;
			}
			else if (len <= 16){
				bits[0] = (byte)(readFromCurrent(8));
				bits[1] = (byte)(readFromCurrent(len - 8) << 16 - len);
				return;
			}
			else if (len <= 24) {
				bits[0] = (byte)(readFromCurrent(8));
				bits[1] = (byte)(readFromCurrent(8));
				bits[2] = (byte)(readFromCurrent(len - 16) << 24 - len);
				return;
			}
			else {
				bits[0] = (byte)(readFromCurrent(8));
				bits[1] = (byte)(readFromCurrent(8));
				bits[2] = (byte)(readFromCurrent(8));
				bits[3] = (byte)(readFromCurrent(len - 24) << 32 - len);
				return;
			}
		}
		else {
			int i, j = 0, b;

			if (fill >= 24) {
				bits[j++] = (byte)(readFromCurrent(8));
				bits[j++] = (byte)(readFromCurrent(8));
				bits[j++] = (byte)(readFromCurrent(8));
				len -= 24;
			}
			else if (fill >= 16) {
				bits[j++] = (byte)(readFromCurrent(8));
				bits[j++] = (byte)(readFromCurrent(8));
				len -= 16;
			}
			else if (fill >= 8) {
				bits[j++] = (byte)(readFromCurrent(8));
				len -= 8;
			}

			final int shift = fill;

			if (shift != 0) {
				bits[j] = (byte)(readFromCurrent(shift) << 8 - shift);
				len -= shift;
				i = len >> 3;
				while(i-- != 0) {
					b = read();
					bits[j] |= (b & 0xFF) >>> shift;
					bits[++j] = (byte)(b << 8 - shift);
				}
			}
			else {
				i = len >> 3;
				while(i-- != 0) bits[j++] = (byte)read();
			}

			readBits += len & ~7;

			len &= 7;
			if (len != 0) {
				if (shift == 0) bits[j] = 0; // We must zero the next byte before OR'ing stuff in
				if (len <= 8 - shift) {
					bits[j] |= (byte)(readFromCurrent(len) << 8 - shift - len);
				}
				else {
					bits[j] |= (byte)(readFromCurrent(8 - shift));
					bits[j + 1] = (byte)(readFromCurrent(len + shift - 8) << 16 - shift - len);
				}
			}
		}
	}



	/** Reads a bit.
	 *
	 * @return the next bit from the stream.
	 */

	public int readBit() throws IOException {
		return readFromCurrent(1);
	}


	/** Reads a fixed number of bits into an integer.
	 *
	 * @param len a bit length.
	 * @return an integer whose lower <code>len</code> bits are taken from the stream; the rest is zeroed.
	 * @throws IOException
	 */

	public int readInt(int len) throws IOException {
		int i, x = 0;

		if (fill < 16) refill();
		if (len <= fill) return readFromCurrent(len);

		len -= fill;
		x = readFromCurrent(fill);

		i = len >> 3;
		while(i-- != 0) x = x << 8 | read();
		readBits += len & ~7;

		len &= 7;

		return (x << len) | readFromCurrent(len);

	}


	/** Reads a fixed number of bits into a long.
	 *
	 * @param len a bit length.
	 * @return a long whose lower <code>len</code> bits are taken from the stream; the rest is zeroed.
	 */

	public long readLong(int len) throws IOException {
		int i;
		long x = 0;

		if (fill < 16) refill();
		if (len <= fill) return readFromCurrent(len);

		len -= fill;
		x = readFromCurrent(fill);

		i = len >> 3;
		while(i-- != 0) x = x << 8 | read();

		len &= 7;

		return (x << len) | readFromCurrent(len);
	}


	/** Skips the given number of bits.
	 *
	 * @param n the number of bits to skip.
	 * @return the actual number of skipped bits.
	 */

	public long skip(long n) throws IOException {
		if (n <= fill) {
			if (n < 0) throw new IllegalArgumentException("Negative bit skip value: " + n);
			fill -= n;
			readBits += n;
			return n;
		}
		else {
			final long prevReadBits = readBits;

			n -= fill;
			readBits += fill;
			fill = 0;

			long nb = n >> 3;

			// TODO: A real evaluation of the usefulness of this block of code
			if (buffer != null && nb > avail && nb < avail + buffer.length) {
				/* If we can skip by simply filling the buffer and skipping some bytes,
				   we do it. Usually the next block has already been fetched by a read-ahead logic. */
				readBits += (avail + 1) << 3;
				n -= (avail + 1) << 3;
				nb -= avail + 1;
				position += pos + avail;
				pos = avail = 0;
				read();
			}

			if (nb <= avail) {
				// We skip bytes directly inside the buffer.
				pos += (int)nb;
				avail -= (int)nb;
				readBits += n & ~7;
			}
			else {
				// No way, we have to pass the byte skip to the underlying stream.
				n -= avail << 3;
				readBits += avail << 3;

				final long toSkip = nb - avail;
				// ALERT: the semantics of skip is flawed--this should be somehow fixed.
				final long skipped = is.skip(toSkip);
				if (skipped < toSkip) throw new IOException("skip() has skipped " + skipped + " instead of " + toSkip + " bytes");

				position += (avail + pos) + skipped;
				pos = 0;
				avail = 0;

				readBits += skipped << 3;

				if (skipped != toSkip) return readBits - prevReadBits;
			}

			final int residual = (int)(n & 7);
			if (residual != 0) {
				current = read();
				fill = 8 - residual;
				readBits += residual;
			}
			return readBits - prevReadBits;
		}
	}

	/** Sets this stream bit position, if it is based on a {@link RepositionableStream} or on a {@link FileChannel}.
	 *
	 * <P>Given an underlying stream that implements {@link
	 * RepositionableStream} or that can provide a {@link
	 * FileChannel} via the <code>getChannel()</code> method,
	 * a call to this method has the same semantics of a {@link #flush()},
	 * followed by a call to {@link
	 * FileChannel#position(long) position(position / 8)} on
	 * the byte stream, followed by a {@link #skip(long) skip(position % 8)}.
	 *
	 * <p>Note that this method does <em>not</em> change the value returned by {@link #readBits()}.
	 *
	 * @param position the new position expressed as a bit offset.
	 * @throws UnsupportedOperationException if the underlying byte stream does not implement
	 * {@link RepositionableStream} or if the channel it returns is not a {@link FileChannel}.
	 * @see FileChannel#position(long)
	 */

    public void position(final long position) throws IOException {
    	if (DEBUG) System.err.println(this + ".position(" + position + ")");

		if (position < 0) throw new IllegalArgumentException("Illegal position: " + position);

		final long bitDelta = ((this.position + pos) << 3) - position;
		if (bitDelta >= 0 && bitDelta <= fill) {
			if (DEBUG) System.err.println("Bit positioning... position: " + position + " this.position: " + this.position + " pos: " + pos + " bitDelta: " + bitDelta + " fill: " + fill);
			fill = (int)bitDelta;
			//System.err.println("Post: " + position + " fill: " + fill);
			return;
		}

		final long delta = (position >> 3) - (this.position + pos);

		if (DEBUG) System.err.println(this + ".position(" + position + "); curr: " + this.position + " delta: " + delta + " pos: " + pos + " avail: " + avail);

		if (delta <= avail && delta >= - pos) {
			// We can reposition just by moving into the buffer.
			avail -= delta;
			pos += delta;
			fill = 0;
			if (DEBUG) System.err.println(this + ": moved internally; pos: " + pos + " avail: " + avail);
		}
		else if (repositionableStream != null) {
			flush();
			repositionableStream.position(this.position = position >> 3);
		}
		else if (fileChannel != null) {
			flush();
			fileChannel.position(this.position = position >> 3);
		}
		else {
			if (wrapping) throw new UnsupportedOperationException("Illegal position: " + position);
			throw new UnsupportedOperationException("position() can only be called if the underlying byte stream implements the RepositionableStream interface or if the getChannel() method of the underlying byte stream exists and returns a FileChannel");
		}

		final int residual = (int)(position & 7);

		if (DEBUG) System.err.println(this + ": residual=" + residual);

		if (residual != 0) {
			current = read();
			fill = 8 - residual;
		}
	}

	/** Returns this stream bit position.
	 * @return  this stream bit position. */
    public long position() {
    	return ((this.position + pos) << 3) - fill;
    }

	/** Tests if this stream supports the {@link #mark(int)} and {@link #reset()} methods.
	 *
	 * <P>This method will just delegate the test to the underlying {@link InputStream}.
	 * @return whether this stream supports {@link #mark(int)}/{@link #reset()}.
	 */

	public boolean markSupported() {
		return is.markSupported();
	}

	/** Marks the current position in this input stream. A subsequent call to
	 * the {@link #reset()} method repositions this stream at the last marked position so
	 * that subsequent reads re-read the same bits.
	 *
	 * <P>This method will just delegate the mark to the underlying {@link InputStream}.
	 * Moreover, it will throw an exception if you try to mark outsite byte boundaries.
	 *
	 * @param readLimit the maximum limit of bytes that can be read before the mark position becomes invalid.
	 * @throws IOException if you try to mark outside byte boundaries.
	 */

	public void mark(final int readLimit) throws IOException {
		if (fill != 0) throw new IOException("You cannot mark a bit stream outside of byte boundaries.");
		is.mark(readLimit);
	}

	/** Repositions this bit stream to the position at the time the {@link #mark(int)} method was last called.
	 *
	 * <P>This method will just {@link #flush() flush the stream} and delegate
	 * the reset to the underlying {@link InputStream}.
	 */

	public void reset() throws IOException {
		flush();
		is.reset();
	}

	/** Reads a natural number in unary coding.
	 *
	 * @return the next unary-encoded natural number.
	 * @see OutputBitStream#writeUnary(int)
	 */

	public int readUnary() throws IOException {
		assert fill < 32 : fill + " >= " + 32;
		int x;

		if (fill < 16) refill();
		x = Integer.numberOfLeadingZeros(current << (32 - fill));
		if (x < fill) { // This works also when fill = 0
			readBits += x + 1;
			fill -= x + 1;
			return x;
		}

		x = fill;
		while((current = read()) == 0) x += 8;
		x += 7 - (fill = 31 - Integer.numberOfLeadingZeros(current));
		readBits += x + 1;
		return x;
	}

	/** Reads a long natural number in unary coding.
	 *
	 * Note that by unary coding we mean that 1 encodes 0, 01 encodes 1 and so on.
	 *
	 * @return the next unary-encoded long natural number.
	 * @see OutputBitStream#writeUnary(int)
	 */

	public long readLongUnary() throws IOException {
		assert fill < 32 : fill + " >= " + 32;

		if (fill < 16) refill();
		long x = Integer.numberOfLeadingZeros(current << (32 - fill));
		if (x < fill) { // This works also when fill = 0
			readBits += x + 1;
			fill -= x + 1;
			return x;
		}

		x = fill;
		while((current = read()) == 0) x += 8;
		x += 7 - (fill = 31 - Integer.numberOfLeadingZeros(current));
		readBits += x + 1;
		return x;
	}

	/** Reads a natural number in &gamma; coding.
	 *
	 * @return the next &gamma;-encoded natural number.
	 * @see OutputBitStream#writeGamma(int)
	 * @see #skipGammas(int)
	 */

	public int readGamma() throws IOException {
		int preComp;
		if ((fill >= 16 || refill() >= 16) && (preComp = GAMMA[current >> (fill - 16) & 0xFFFF]) != 0) {
			readBits += preComp >> 16;
			fill -= preComp >> 16;
			return preComp & 0xFFFF;
		}

		final int msb = readUnary();
		return ((1 << msb) | readInt(msb)) - 1;
	}

	/** Reads a long natural number in &gamma; coding.
	 *
	 * @return the next &gamma;-encoded long natural number.
	 * @see OutputBitStream#writeGamma(int)
	 * @see #skipGammas(int)
	 */

	public long readLongGamma() throws IOException {
		int preComp;
		if ((fill >= 16 || refill() >= 16) && (preComp = GAMMA[current >> (fill - 16) & 0xFFFF]) != 0) {
			readBits += preComp >> 16;
			fill -= preComp >> 16;
			return preComp & 0xFFFF;
		}

		final int msb = readUnary();
		return ((1L << msb) | readLong(msb)) - 1;
	}

	/** Skips a given amount of &gamma;-coded natural numbers.
	 *
	 * <p>This method should be significantly quicker than iterating <code>n</code> times on
	 * {@link #readGamma()} or {@link #readLongGamma()}, as precomputed tables are used directly,
	 * so the number of method calls is greatly reduced, and the result is discarded, so
	 * {@link #skip(long)} can be invoked instead of more specific decoding methods.
	 *
	 * @param n the number of &gamma;-coded natural numbers to be skipped.
	 * @see #readGamma()
	 */

	public void skipGammas(long n) throws IOException {
		int preComp;
		while(n-- != 0) {
			if ((fill >= 16 || refill() >= 16) && (preComp = GAMMA[current >> (fill - 16) & 0xFFFF] >> 16) != 0) {
				readBits += preComp;
				fill -= preComp;
				continue;
			}

			skip((long)readUnary());
		}
	}

	/** Skips a given amount of &gamma;-coded natural numbers.
	 *
	 * <p>This method should be significantly quicker than iterating <code>n</code> times on
	 * {@link #readGamma()} or {@link #readLongGamma()}, as precomputed tables are used directly,
	 * so the number of method calls is greatly reduced, and the result is discarded, so
	 * {@link #skip(long)} can be invoked instead of more specific decoding methods.
	 *
	 * @param n the number of &gamma;-coded natural numbers to be skipped.
	 * @see #readGamma()
	 */
	public void skipGammas(final int n) throws IOException {
		skipGammas((long)n);
	}

	/** Reads a given amount of &gamma;-coded natural numbers.
	 *
	 * <p>This method should be significantly quicker than iterating <code>n</code> times on
	 * {@link #readGamma()}, as precomputed tables are used directly,
	 * so the number of method calls is greatly reduced.
	 *
	 * @param a an array of at least <code>count</code> integers where the result
	 * will be written starting at the first position.
	 * @param count the number of &gamma;-coded natural numbers to be read.
	 * @see #readGamma()
	 */

	public void readGammas(final int[] a, final int count) throws IOException {
		int preComp, msb;
		for(int i = 0; i < count; i++) {
			if ((fill >= 16 || refill() >= 16) && (preComp = GAMMA[current >> (fill - 16) & 0xFFFF]) != 0) {
				readBits += preComp >> 16;
				fill -= preComp >> 16;
				a[i] = preComp & 0xFFFF;
				continue;
			}

			a[i] = ((1 << (msb = readUnary())) | readInt(msb)) - 1;
		}
	}

	/** Reads a natural number in shifted &gamma; coding.
	 *
	 * @return the next shifted-&gamma;&ndash;encoded natural number.
	 * @see OutputBitStream#writeShiftedGamma(int)
	 * @see #skipShiftedGammas(int)
	 */

	public int readShiftedGamma() throws IOException {
		int preComp;
		if ((fill >= 16 || refill() >= 16) && (preComp = SHIFTED_GAMMA[current >> (fill - 16) & 0xFFFF]) != 0) {
			readBits += preComp >> 16;
			fill -= preComp >> 16;
			return preComp & 0xFFFF;
		}

		final int msb = readUnary() - 1;
		return msb == -1 ? 0 : ((1 << msb) | readInt(msb));
	}

	/** Reads a natural number in shifted &gamma; coding.
	 *
	 * @return the next shifted-&gamma;&ndash;encoded natural number.
	 * @see OutputBitStream#writeShiftedGamma(int)
	 * @see #skipShiftedGammas(int)
	 */

	public long readLongShiftedGamma() throws IOException {
		int preComp;
		if ((fill >= 16 || refill() >= 16) && (preComp = SHIFTED_GAMMA[current >> (fill - 16) & 0xFFFF]) != 0) {
			readBits += preComp >> 16;
			fill -= preComp >> 16;
			return preComp & 0xFFFF;
		}

		final int msb = readUnary() - 1;
		return msb == -1 ? 0 : ((1L << msb) | readLong(msb));
	}

	/** Skips a given amount of shifted-&gamma;-coded natural numbers.
	 *
	 * <p>This method should be significantly quicker than iterating <code>n</code> times on
	 * {@link #readShiftedGamma()} or {@link #readLongShiftedGamma()}, as precomputed tables are used directly,
	 * so the number of method calls is greatly reduced, and the result is discarded, so
	 * {@link #skip(long)} can be invoked instead of more specific decoding methods.
	 *
	 * @param n the number of shifted-&gamma;-coded natural numbers to be skipped.
	 * @see #readShiftedGamma()
	 */

	public void skipShiftedGammas(long n) throws IOException {
		int preComp;
		while(n-- != 0) {
			if ((fill >= 16 || refill() >= 16) && (preComp = SHIFTED_GAMMA[current >> (fill - 16) & 0xFFFF] >> 16) != 0) {
				readBits += preComp;
				fill -= preComp;
				continue;
			}

			final long msb = readUnary() - 1;
			if (msb > 0) skip(msb);
		}
	}

	/** Skips a given amount of shifted-&gamma;-coded natural numbers.
	 *
	 * <p>This method should be significantly quicker than iterating <code>n</code> times on
	 * {@link #readShiftedGamma()} or {@link #readLongShiftedGamma()}, as precomputed tables are used directly,
	 * so the number of method calls is greatly reduced, and the result is discarded, so
	 * {@link #skip(long)} can be invoked instead of more specific decoding methods.
	 *
	 * @param n the number of shifted-&gamma;-coded natural numbers to be skipped.
	 * @see #readShiftedGamma()
	 */

	public void skipShiftedGammas(final int n) throws IOException {
		skipShiftedGammas((long)n);
	}


	/** Reads a given amount of shifted-&gamma;-coded natural numbers.
	 *
	 * <p>This method should be significantly quicker than iterating <code>n</code> times on
	 * {@link #readShiftedGamma()}, as precomputed tables are used directly,
	 * so the number of method calls is greatly reduced.
	 *
	 * @param a an array of at least <code>count</code> integers where the result
	 * will be written starting at the first position.
	 * @param count the number of shifted-&gamma;-coded natural numbers to be read.
	 * @see #readShiftedGamma()
	 */

	public void readShiftedGammas(final int[] a, final int count) throws IOException {
		int preComp, msb;
		for(int i = 0; i < count; i++) {
			if ((fill >= 16 || refill() >= 16) && (preComp = SHIFTED_GAMMA[current >> (fill - 16) & 0xFFFF]) != 0) {
				readBits += preComp >> 16;
				fill -= preComp >> 16;
				a[i] = preComp & 0xFFFF;
				continue;
			}

			msb = readUnary() - 1;
			a[i] = msb == -1 ? 0 : ((1 << msb) | readInt(msb));
		}
	}

	/** Reads a natural number in &delta; coding.
	 *
	 * @return the next &delta;-encoded natural number.
	 * @see OutputBitStream#writeDelta(int)
	 * @see #skipDeltas(int)
	 */

	public int readDelta() throws IOException {
		int preComp;
		if ((fill >= 16 || refill() >= 16) && (preComp = DELTA[current >> (fill - 16) & 0xFFFF]) != 0) {
			readBits += preComp >> 16;
			fill -= preComp >> 16;
			return preComp & 0xFFFF;
		}

		final int msb = readGamma();
		return ((1 << msb) | readInt(msb)) - 1;
	}


	/** Reads a long natural number in &delta; coding.
	 *
	 * @return the next &delta;-encoded long natural number.
	 * @see OutputBitStream#writeDelta(int)
	 * @see #skipDeltas(int)
	 */

	public long readLongDelta() throws IOException {
		int preComp;
		if ((fill >= 16 || refill() >= 16) && (preComp = DELTA[current >> (fill - 16) & 0xFFFF]) != 0) {
			readBits += preComp >> 16;
			fill -= preComp >> 16;
			return preComp & 0xFFFF;
		}

		final int msb = readGamma();
		return ((1L << msb) | readLong(msb)) - 1;
	}


	/** Skips a given amount of &delta;-coded natural numbers.
	 *
	 * <p>This method should be significantly quicker than iterating <code>n</code> times on
	 * {@link #readDelta()} or {@link #readLongDelta()}, as precomputed tables are used directly,
	 * so the number of method calls is greatly reduced, and the result is discarded, so
	 * {@link #skip(long)} can be invoked instead of more specific decoding methods.
	 *
	 * @param n the number of &delta;-coded natural numbers to be skipped.
	 * @see #readDelta()
	 */

	public void skipDeltas(long n) throws IOException {
		int preComp;
		while(n-- != 0) {
			if ((fill >= 16 || refill() >= 16) && (preComp = DELTA[current >> (fill - 16) & 0xFFFF] >> 16) != 0) {
				readBits += preComp;
				fill -= preComp;
				continue;
			}

			skip((long)readGamma());
		}
	}

	/** Skips a given amount of &delta;-coded natural numbers.
	 *
	 * <p>This method should be significantly quicker than iterating <code>n</code> times on
	 * {@link #readDelta()} or {@link #readLongDelta()}, as precomputed tables are used directly,
	 * so the number of method calls is greatly reduced, and the result is discarded, so
	 * {@link #skip(long)} can be invoked instead of more specific decoding methods.
	 *
	 * @param n the number of &delta;-coded natural numbers to be skipped.
	 * @see #readDelta()
	 */

	public void skipDeltas(final int n) throws IOException {
		skipDeltas((long)n);
	}

	/** Reads a given amount of &delta;-coded natural numbers.
	 *
	 * <p>This method should be significantly quicker than iterating <code>n</code> times on
	 * {@link #readDelta()}, as precomputed tables are used directly,
	 * so the number of method calls is greatly reduced.
	 *
	 * @param a an array of at least <code>count</code> integers where the result
	 * will be written starting at the first position.
	 * @param count the number of &delta;-coded natural numbers to be read.
	 * @see #readDelta()
	 */

	public void readDeltas(final int[] a, final int count) throws IOException {
		int preComp, msb;
		for(int i = 0; i < count; i++) {
			if ((fill >= 16 || refill() >= 16) && (preComp = DELTA[current >> (fill - 16) & 0xFFFF]) != 0) {
				readBits += preComp >> 16;
				fill -= preComp >> 16;
				a[i] = preComp & 0xFFFF;
				continue;
			}

			a[i]  = ((1 << (msb = readGamma())) | readInt(msb)) - 1;
		}
	}

	/** Reads a natural number in a limited range using a minimal binary coding.
	 *
	 * @param b a strict upper bound.
	 * @return the next minimally binary encoded natural number.
	 * @throws IllegalArgumentException if you try to read a negative number or use a nonpositive base.
	 * @see OutputBitStream#writeMinimalBinary(int, int)
	 */

	public int readMinimalBinary(final int b) throws IOException {
		return readMinimalBinary(b, Fast.mostSignificantBit(b));
	}

	/** Reads a natural number in a limited range using a minimal binary coding.
	 *
	 * This method is faster than {@link #readMinimalBinary(int)} because it does not
	 * have to compute <code>log2b</code>.
	 *
	 * @param b a strict upper bound.
	 * @param log2b the floor of the base-2 logarithm of the bound.
	 * @return the next minimally binary encoded natural number.
	 * @throws IllegalArgumentException if you try to read a negative number or use a nonpositive base.
	 * @see OutputBitStream#writeMinimalBinary(int, int)
	 */

	public int readMinimalBinary(final int b, final int log2b) throws IOException {
		if (b < 1) throw new IllegalArgumentException("The bound " + b + " is not positive");

		final int m = (1 << log2b + 1) - b;
		final int x = readInt(log2b);

		if (x < m) return x;
		else return ((x << 1) + readBit() - m);
	}

	/** Reads a long natural number in a limited range using a minimal binary coding.
	 *
	 * @param b a strict upper bound.
	 * @return the next minimally binary encoded long natural number.
	 * @throws IllegalArgumentException if you try to read a negative number or use a nonpositive base.
	 * @see OutputBitStream#writeMinimalBinary(int, int)
	 */

	public long readLongMinimalBinary(final long b) throws IOException {
		return readLongMinimalBinary(b, Fast.mostSignificantBit(b));
	}


	/** Reads a long natural number in a limited range using a minimal binary coding.
	 *
	 * This method is faster than {@link #readLongMinimalBinary(long)} because it does not
	 * have to compute <code>log2b</code>.
	 *
	 * @param b a strict upper bound.
	 * @param log2b the floor of the base-2 logarithm of the bound.
	 * @return the next minimally binary encoded long natural number.
	 * @throws IllegalArgumentException if you try to read a negative number or use a nonpositive base.
	 * @see OutputBitStream#writeMinimalBinary(int, int)
	 */

	public long readLongMinimalBinary(final long b, final int log2b) throws IOException {
		if (b < 1) throw new IllegalArgumentException("The bound " + b + " is not positive");

		final long m = (1L << log2b + 1) - b;
		final long x = readLong(log2b);

		if (x < m) return x;
		else return ((x << 1) + readBit() - m);
	}

	/** Reads a natural number in Golomb coding.
	 *
	 * <P>This method implements also the case in which <code>b</code> is 0: in this case,
	 * nothing will be read, and 0 will be returned.
	 *
	 * @param b the modulus for the coding.
	 * @return the next Golomb-encoded natural number.
	 * @throws IllegalArgumentException if you use a nonpositive modulus.
	 * @see OutputBitStream#writeGolomb(int, int)
	 */

	public int readGolomb(final int b) throws IOException {
		return readGolomb(b, Fast.mostSignificantBit(b));
	}

	/** Reads a natural number in Golomb coding.
	 *
	 * This method is faster than {@link #readGolomb(int)} because it does not
	 * have to compute <code>log2b</code>.
	 *
	 * <P>This method implements also the case in which <code>b</code> is 0: in this case,
	 * nothing will be read, and 0 will be returned.
	 *
	 * @param b the modulus for the coding.
	 * @param log2b the floor of the base-2 logarithm of the coding modulus.
	 * @return the next Golomb-encoded natural number.
	 * @throws IllegalArgumentException if you use a nonpositive modulus.
	 * @see OutputBitStream#writeGolomb(int, int)
	 */

	public int readGolomb(final int b, final int log2b) throws IOException {
		if (b < 0) throw new IllegalArgumentException("The modulus " + b + " is negative");
		if (b == 0) return 0;

		return readUnary() * b + readMinimalBinary(b, log2b);
	}



	/** Reads a long natural number in Golomb coding.
	 *
	 * <P>This method implements also the case in which <code>b</code> is 0: in this case,
	 * nothing will be read, and 0 will be returned.
	 *
	 * @param b the modulus for the coding.
	 * @return the next Golomb-encoded long natural number.
	 * @throws IllegalArgumentException if you use a nonpositive modulus.
	 * @see OutputBitStream#writeGolomb(int, int)
	 */

	public long readLongGolomb(final long b) throws IOException {
		return readLongGolomb(b, Fast.mostSignificantBit(b));
	}

	/** Reads a long natural number in Golomb coding.
	 *
	 * This method is faster than {@link #readLongGolomb(long)} because it does not
	 * have to compute <code>log2b</code>.
	 *
	 * <P>This method implements also the case in which <code>b</code> is 0: in this case,
	 * nothing will be read, and 0 will be returned.
	 *
	 * @param b the modulus for the coding.
	 * @param log2b the floor of the base-2 logarithm of the coding modulus.
	 * @return the next Golomb-encoded long natural number.
	 * @throws IllegalArgumentException if you use a nonpositive modulus.
	 * @see OutputBitStream#writeGolomb(int, int)
	 */

	public long readLongGolomb(final long b, final int log2b) throws IOException {
		if (b < 0) throw new IllegalArgumentException("The modulus " + b + " is negative");
		if (b == 0) return 0;

		return readUnary() * b + readLongMinimalBinary(b, log2b);
	}

	/** Reads a natural number in skewed Golomb coding.
	 *
	 * <P>This method implements also the case in which <code>b</code> is 0: in this case,
	 * nothing will be read, and 0 will be returned.
	 *
	 * @param b the modulus for the coding.
	 * @return the next skewed Golomb-encoded natural number.
	 * @throws IllegalArgumentException if you use a negative modulus.
	 * @see OutputBitStream#writeSkewedGolomb(int, int)
	 */

	public int readSkewedGolomb(final int b) throws IOException {
		if (b < 0) throw new IllegalArgumentException("The modulus " + b + " is negative");
		if (b == 0) return 0;

		final int M = ((1 << readUnary() + 1) - 1) * b;
		final int m = (M / (2 * b)) * b;
		return m + readMinimalBinary(M - m);
	}


	/** Reads a long natural number in skewed Golomb coding.
	 *
	 * <P>This method implements also the case in which <code>b</code> is 0: in this case,
	 * nothing will be read, and 0 will be returned.
	 *
	 * @param b the modulus for the coding.
	 * @return the next skewed Golomb-encoded long natural number.
	 * @throws IllegalArgumentException if you use a negative modulus.
	 * @see OutputBitStream#writeSkewedGolomb(int, int)
	 */

	public long readLongSkewedGolomb(final long b) throws IOException {
		if (b < 0) throw new IllegalArgumentException("The modulus " + b + " is negative");
		if (b == 0) return 0;

		final long M = ((1L << readUnary() + 1) - 1) * b;
		final long m = (M / (2 * b)) * b;
		return m + readLongMinimalBinary(M - m);
	}


	/** Reads a natural number in &zeta; coding.
	 *
	 * @param k the shrinking factor.
	 * @return the next &zeta;-encoded natural number.
	 * @throws IllegalArgumentException if you use a nonpositive shrinking factor.
	 * @see OutputBitStream#writeZeta(int, int)
	 */

	public int readZeta(final int k) throws IOException {
		if (k < 1) throw new IllegalArgumentException("The shrinking factor " + k + " is not positive");

		if (k == 3) {
			int preComp;
			if ((fill >= 16 || refill() >= 16) && (preComp = ZETA_3[current >> (fill - 16) & 0xFFFF]) != 0) {
				readBits += preComp >> 16;
				fill -= preComp >> 16;
				return preComp & 0xFFFF;
			}
		}

		final int h = readUnary();
		final int left = 1 << h * k;
		final int m = readInt(h * k + k - 1);
		if (m < left) return m + left - 1;
		return (m << 1) + readBit() - 1;
	}

	/** Reads a long natural number in &zeta; coding.
	 *
	 * @param k the shrinking factor.
	 * @return the next &zeta;-encoded long natural number.
	 * @throws IllegalArgumentException if you use a nonpositive shrinking factor.
	 * @see OutputBitStream#writeZeta(int, int)
	 */

	public long readLongZeta(final int k) throws IOException {
		if (k < 1) throw new IllegalArgumentException("The shrinking factor " + k + " is not positive");

		if (k == 3) {
			int preComp;
			if ((fill >= 16 || refill() >= 16) && (preComp = ZETA_3[current >> (fill - 16) & 0xFFFF]) != 0) {
				readBits += preComp >> 16;
				fill -= preComp >> 16;
				return preComp & 0xFFFF;
			}
		}

		final int h = readUnary();
		final long left = 1L << h * k;
		final long m = readLong(h * k + k - 1);
		if (m < left) return m + left - 1;
		return (m << 1) + readBit() - 1;
	}


	/** Skips a given amount of &zeta;-coded natural numbers.
	 *
	 * <p>This method should be significantly quicker than iterating <code>n</code> times on
	 * {@link #readZeta(int)} or {@link #readLongZeta(int)}, as precomputed tables are used directly,
	 * so the number of method calls is greatly reduced, and the result is discarded, so
	 * {@link #skip(long)} can be invoked instead of more specific decoding methods.
	 *
	 *
	 * @param k the shrinking factor.
	 * @param n the number of &zeta;-coded natural numbers to be skipped.
	 * @see #readZeta(int)
	 */

	public void skipZetas(final int k, long n) throws IOException {
		int h, preComp;

		while(n-- != 0) {
			if (k == 3 && (fill >= 16 || refill() >= 16) && (preComp = ZETA_3[current >> (fill - 16) & 0xFFFF] >> 16) != 0) {
				readBits += preComp;
				fill -= preComp;
				continue;
			}

			h = readUnary();
			if (readInt(h * k + k - 1) >= 1 << h * k) skip(1L);
		}
	}

	/** Skips a given amount of &zeta;-coded natural numbers.
	 *
	 * <p>This method should be significantly quicker than iterating <code>n</code> times on
	 * {@link #readZeta(int)} or {@link #readLongZeta(int)}, as precomputed tables are used directly,
	 * so the number of method calls is greatly reduced, and the result is discarded, so
	 * {@link #skip(long)} can be invoked instead of more specific decoding methods.
	 *
	 *
	 * @param k the shrinking factor.
	 * @param n the number of &zeta;-coded natural numbers to be skipped.
	 * @see #readZeta(int)
	 */

	public void skipZetas(final int k, final int n) throws IOException {
		skipZetas(k, (long)n);
	}

	/** Reads a given amount of &gamma;-coded natural numbers.
	 *
	 * <p>This method should be significantly quicker than iterating <code>n</code> times on
	 * {@link #readGamma()}, as precomputed tables are used directly,
	 * so the number of method calls is greatly reduced.
	 *
	 * @param k the shrinking factor.
	 * @param a an array of at least <code>count</code> integers where the result
	 * will be written starting at the first position.
	 * @param count the number of &zeta;-coded natural numbers to be read.
	 * @see #readGamma()
	 */

	public void readZetas(final int k, final int[] a, final int count) throws IOException {
		int h, left, m;
		int preComp;

		for(int i = 0; i < count; i++) {
			if (k == 3 && (fill >= 16 || refill() >= 16) && (preComp = ZETA_3[current >> (fill - 16) & 0xFFFF]) != 0) {
				readBits += preComp >> 16;
				fill -= preComp >> 16;
				a[i] = preComp & 0xFFFF;
				continue;
			}

			h = readUnary();
			left = 1 << h * k;
			m = readInt(h * k + k - 1);
			a[i] = m < left ? m + left - 1 : (m << 1) + readBit() - 1;
		}
	}

	/** Reads a natural number in variable-length nibble coding.
	 *
	 * @return the next variable-length nibble-encoded natural number.
	 * @see OutputBitStream#writeNibble(int)
	 */

	public int readNibble() throws IOException {
		int b;
		int x = 0;

		do {
			x <<= 3;
			b = readBit();
			x |= readInt(3);
		} while(b == 0);

		return x;
	}

	/** Reads a long natural number in variable-length nibble coding.
	 *
	 * @return the next variable-length nibble-encoded long natural number.
	 * @see OutputBitStream#writeNibble(int)
	 */

	public long readLongNibble() throws IOException {
		int b;
		long x = 0;

		do {
			x <<= 3;
			b = readBit();
			x |= readInt(3);
		} while(b == 0);

		return x;
	}

	@Override
	public boolean hasNext() {
		return true;
	}

	@Override
	public boolean nextBoolean() {
		try {
			return readBit() != 0;
		}
		catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	/** Skips over the given number of bits.
	 *
	 * @param n the number of bits to skip.
	 * @return the number of bits actually skipped.
	 * @deprecated This method is simply an expensive, try/catch-surrounded version
	 * of {@link #skip(long)} that is made necessary by the interface
	 * by {@link BooleanIterator}.
	 */
	@Override
	@Deprecated
	public int skip(final int n) {
		try {
			return (int)skip((long)n);
		}
		catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	/** Copies a given number of bits from this input bit stream into a given output bit stream.
	 *
	 * @param obs an output bit stream.
	 * @param length the number of bits to copy.
	 * @throws EOFException if there are not enough bits to copy.
	 */
	public void copyTo(final OutputBitStream obs, long length) throws IOException {
		final byte[] buffer = new byte[64 * 1024];
		while(length > 0) {
			final int toRead = (int)Math.min(length, buffer.length * Byte.SIZE);
			read(buffer, toRead);
			obs.write(buffer, 0, toRead);
			length -= toRead;
		}
	}
}
