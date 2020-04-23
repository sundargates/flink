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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.io.BinaryData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.flink.core.memory.DataOutputView;

public class DataOutputEncoder extends BinaryEncoder {
	private byte[] buf;
	private int pos;
	private ByteSink sink;
	private int bulkLimit;

	private static final int DEFAULT_BUFFER_SIZE = 2048;

	public void configure(DataOutputView dataOutputView) {
		configure(dataOutputView, DEFAULT_BUFFER_SIZE);
	}

	public void configure(DataOutputView dataOutputView, int bufferSize) {
		if (null == dataOutputView) throw new NullPointerException("OutputStream cannot be null!");

		if (null != this.sink) {
			if (pos > 0) {
				try {
					flushBuffer();
				} catch (IOException e) {
					throw new AvroRuntimeException("Failure flushing old output", e);
				}
			}
		}
		this.sink = new DataOutputViewStreamSink(dataOutputView);
		pos = 0;
		if (null == buf || buf.length != bufferSize) {
			buf = new byte[bufferSize];
		}

		bulkLimit = buf.length >>> 1;
		if (bulkLimit > 512) {
			bulkLimit = 512;
		}
	}

	@Override
	public int bytesBuffered() {
		return pos;
	}

	@Override
	public void writeBoolean(boolean b) throws IOException {
		// inlined, shorter version of ensureBounds
		if (buf.length == pos) {
			flushBuffer();
		}
		pos += BinaryData.encodeBoolean(b, buf, pos);
	}

	@Override
	public void writeInt(int n) throws IOException {
		ensureBounds(5);
		pos += BinaryData.encodeInt(n, buf, pos);
	}

	@Override
	public void writeLong(long n) throws IOException {
		ensureBounds(10);
		pos += BinaryData.encodeLong(n, buf, pos);
	}

	@Override
	public void writeFloat(float f) throws IOException {
		ensureBounds(4);
		pos += BinaryData.encodeFloat(f, buf, pos);
	}

	@Override
	public void writeDouble(double d) throws IOException {
		ensureBounds(8);
		pos += BinaryData.encodeDouble(d, buf, pos);
	}

	@Override
	public void writeFixed(byte[] bytes, int start, int len) throws IOException {
		if (len > bulkLimit) {
			// too big, write direct
			flushBuffer();
			sink.innerWrite(bytes, start, len);
			return;
		}
		ensureBounds(len);
		System.arraycopy(bytes, start, buf, pos, len);
		pos += len;
	}

	@Override
	public void writeFixed(ByteBuffer bytes) throws IOException {
		if (!bytes.hasArray() && bytes.remaining() > bulkLimit) {
			flushBuffer();
			sink.innerWrite(bytes); // bypass the buffer
		} else {
			super.writeFixed(bytes);
		}
	}

	@Override
	protected void writeZero() throws IOException {
		writeByte(0);
	}

	private void writeByte(int b) throws IOException {
		if (pos == buf.length) {
			flushBuffer();
		}
		buf[pos++] = (byte) (b & 0xFF);
	}

	@Override
	public void flush() throws IOException {
		flushBuffer();
		sink.innerFlush();
	}

	/** Flushes the internal buffer to the underlying output. Does not flush the underlying output. */
	private void flushBuffer() throws IOException {
		if (pos > 0) {
			sink.innerWrite(buf, 0, pos);
			pos = 0;
		}
	}

	private void ensureBounds(int num) throws IOException {
		int remaining = buf.length - pos;
		if (remaining < num) {
			flushBuffer();
		}
	}

	private abstract static class ByteSink {
		protected ByteSink() {}
		/** Write data from bytes, starting at off, for len bytes * */
		protected abstract void innerWrite(byte[] bytes, int off, int len) throws IOException;

		protected abstract void innerWrite(ByteBuffer buff) throws IOException;

		/** Flush the underlying output, if supported * */
		protected abstract void innerFlush() throws IOException;
	}

	static class DataOutputViewStreamSink extends ByteSink {
		private final DataOutputView out;

		private DataOutputViewStreamSink(DataOutputView out) {
			super();
			this.out = out;
		}

		@Override
		protected void innerWrite(byte[] bytes, int off, int len) throws IOException {
			out.write(bytes, off, len);
		}

		@Override
		protected void innerFlush() throws IOException {}

		@Override
		protected void innerWrite(ByteBuffer buff) throws IOException {
			out.write(buff.array());
		}
	}
}

