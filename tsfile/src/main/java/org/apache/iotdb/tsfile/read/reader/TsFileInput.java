package org.apache.iotdb.tsfile.read.reader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.*;

public interface TsFileInput {
    /**
     * Returns the current size of this input.
     *
     * @return  The current size of this input,
     *          measured in bytes
     *
     * @throws  ClosedChannelException
     *          If this channel is closed
     *
     * @throws IOException
     *          If some other I/O error occurs
     */
    long size() throws IOException;

    /**
     * Returns this input's current position.
     *
     * @return  This input's current position,
     *          a non-negative integer counting the number of bytes
     *          from the beginning of the input to the current position
     *
     * @throws  ClosedChannelException
     *          If this input is closed
     *
     * @throws  IOException
     *          If some other I/O error occurs
     */
    long position() throws IOException;

    /**
     * Sets this input's position.
     *
     * <p> Setting the position to a value that is greater than the input's
     * current size is legal but does not change the size of the TsFileInput.  A later
     * attempt to read bytes at such a position will immediately return an
     * end-of-file indication.</p>
     *
     * @param  newPosition
     *         The new position, a non-negative integer counting
     *         the number of bytes from the beginning of the TsFileInput
     *
     * @return  This TsFileInput
     *
     * @throws  ClosedChannelException
     *          If this TsFileInput is closed
     *
     * @throws  IllegalArgumentException
     *          If the new position is negative
     *
     * @throws  IOException
     *          If some other I/O error occurs
     */
    TsFileInput position(long newPosition) throws IOException;


    /**
     * Reads a sequence of bytes from this TsFileInput into the given buffer.
     *
     * <p> Bytes are read starting at this TsFileInput's current  position, and
     * then the  position is updated with the number of bytes actually
     * read.  Otherwise this method behaves exactly as specified in the {@link
     * ReadableByteChannel} interface. </p>
     */
    int read(ByteBuffer dst) throws IOException;

    /**
     * Reads a sequence of bytes from this TsFileInput into the given buffer,
     * starting at the given  position.
     *
     * <p> This method works in the same manner as the {@link
     * #read(ByteBuffer)} method, except that bytes are read starting at the
     * given  position rather than at the TsFileInput's current position.  This
     * method does not modify this TsFileInput's position.  If the given position
     * is greater than the TsFileInput's current size then no bytes are read.  </p>
     *
     * @param  dst
     *         The buffer into which bytes are to be transferred
     *
     * @param  position
     *         The position at which the transfer is to begin;
     *         must be non-negative
     *
     * @return  The number of bytes read, possibly zero, or <tt>-1</tt> if the
     *          given position is greater than or equal to the file's current
     *          size
     *
     * @throws  IllegalArgumentException
     *          If the position is negative
     *
     * @throws  ClosedChannelException
     *          If this TsFileInput is closed
     *
     * @throws AsynchronousCloseException
     *          If another thread closes this TsFileInput
     *          while the read operation is in progress
     *
     * @throws ClosedByInterruptException
     *          If another thread interrupts the current thread
     *          while the read operation is in progress, thereby
     *          closing the channel and setting the current thread's
     *          interrupt status
     *
     * @throws  IOException
     *          If some other I/O error occurs
     */
    int read(ByteBuffer dst, long position) throws IOException;

    FileChannel wrapAsFileChannel() throws IOException;

    InputStream wrapAsInputStream() throws IOException;

    /**
     * Closes this channel.
     *
     * <p> If the channel has already been closed then this method returns
     * immediately. </p>
     *
     * @throws  IOException
     *          If an I/O error occurs
     */
    void close() throws IOException;

    /**
     * read a byte from the Input
     * @return
     * @throws IOException
     */
    int read() throws IOException;

    /**
     *
     * @param b
     * @param off
     * @param len
     * @return
     * @throws IOException
     */
    int read(byte[] b, int off, int len) throws IOException;

    /**
     * read 4 bytes from the Input and convert it to a integer
     * @return
     * @throws IOException
     */
    int readInt() throws IOException;
}
