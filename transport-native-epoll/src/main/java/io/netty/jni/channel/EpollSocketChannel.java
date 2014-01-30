/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.jni.channel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.EventLoop;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.jni.internal.Native;
import io.netty.util.internal.StringUtil;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public final class EpollSocketChannel extends AbstractEpollChannel implements SocketChannel {
    private final EpollSocketChannelConfig config;

    /**
     * The future of the current connection attempt.  If not null, subsequent
     * connection attempts will fail.
     */
    private ChannelPromise connectPromise;
    private ScheduledFuture<?> connectTimeoutFuture;
    private SocketAddress requestedRemoteAddress;

    private volatile boolean inputShutdown;
    private volatile boolean outputShutdown;

    EpollSocketChannel(Channel parent, int fd) {
        super(parent, fd);
        active = true;
        config = new EpollSocketChannelConfig(this);
    }

    public EpollSocketChannel() {
        config = new EpollSocketChannelConfig(this);
    }

    @Override
    protected NativeUnsafe newUnsafe() {
        return new NativeUnsafeImpl();
    }

    @Override
    protected SocketAddress localAddress0() {
        return Native.localAddress(fd);
    }

    @Override
    protected SocketAddress remoteAddress0() {
        return Native.remoteAddress(fd);
    }

    @Override
    protected void doBind(SocketAddress local) throws Exception {
        InetSocketAddress localAddress = (InetSocketAddress) local;
        Native.bind(fd, localAddress.getAddress(), localAddress.getPort());
    }

    private void setEpollOut() {
        if ((flags & Native.EPOLLOUT) == 0) {
            flags |= Native.EPOLLOUT;
            ((EpollEventLoop) eventLoop()).modify(this);
        }
    }

    private void clearEpollOut() {
        if ((flags & Native.EPOLLOUT) != 0) {
            flags = Native.EPOLLOUT;
            ((EpollEventLoop) eventLoop()).modify(this);
        }
    }

    /**
     * Read bytes into the given {@link ByteBuf} and return the amount.
     */
    private int doReadBytes(ByteBuf byteBuf) throws Exception {
        ByteBuffer buf = byteBuf.internalNioBuffer(0, byteBuf.writableBytes());
        int localReadAmount = Native.read(fd, buf, buf.position(), buf.limit());
        if (localReadAmount > 0) {
            byteBuf.writerIndex(byteBuf.writerIndex() + localReadAmount);
        }
        return localReadAmount;
    }

    /**
     * Write bytes form the given {@link ByteBuf} to the underlying {@link java.nio.channels.Channel}.
     * @param buf           the {@link ByteBuf} from which the bytes should be written
     * @return amount       the amount of written bytes
     */
    private int doWriteBytes(ByteBuf buf, int readable) throws Exception {
        int readerIndex = buf.readerIndex();
        int localFlushedAmount;
        if (buf.nioBufferCount() == 1) {
            ByteBuffer nioBuf = buf.internalNioBuffer(readerIndex, readable);
            localFlushedAmount = Native.write(fd, nioBuf, nioBuf.position(), nioBuf.limit());
        } else {
            // backed by more then one buffer, do a gathering write...
            ByteBuffer[] nioBufs = buf.nioBuffers();
            localFlushedAmount = (int) Native.writev(fd, nioBufs, 0, nioBufs.length);
        }
        if (localFlushedAmount > 0) {
            buf.readerIndex(readerIndex + localFlushedAmount);
        }
        return localFlushedAmount;
    }

    /**
     * Write a {@link DefaultFileRegion}
     *
     * @param region        the {@link DefaultFileRegion} from which the bytes should be written
     * @return amount       the amount of written bytes
     */
    private long doWriteFileRegion(DefaultFileRegion region, long count) throws Exception {
        return Native.sendfile(fd, region, region.transfered(), count);
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {
        for (;;) {
            final int msgCount = in.size();

            if (msgCount == 0) {
                // Wrote all messages.
                clearEpollOut();
                break;
            }
            // Do non-gathering write for a single buffer case.
            if (msgCount > 1) {
                // Ensure the pending writes are made of ByteBufs only.
                ByteBuffer[] nioBuffers = in.nioBuffers();
                if (nioBuffers != null) {

                    int nioBufferCnt = in.nioBufferCount();
                    long expectedWrittenBytes = in.nioBufferSize();

                    long localWrittenBytes = Native.writev(fd, nioBuffers, 0, nioBufferCnt);

                    if (localWrittenBytes < expectedWrittenBytes) {
                        int nioBufIndex = 0;
                        setEpollOut();

                        // Did not write all buffers completely.
                        // Release the fully written buffers and update the indexes of the partially written buffer.
                        for (int i = msgCount; i > 0; i --) {
                            final ByteBuf buf = (ByteBuf) in.current();
                            final int readerIndex = buf.readerIndex();
                            final int readableBytes = buf.writerIndex() - readerIndex;

                            if (readableBytes < localWrittenBytes) {
                                nioBufIndex += buf.nioBufferCount();
                                in.remove();
                                localWrittenBytes -= readableBytes;
                            } else if (readableBytes > localWrittenBytes) {

                                buf.readerIndex(readerIndex + (int) localWrittenBytes);
                                in.progress(localWrittenBytes);

                                // update position in ByteBuffer as we not do this in the native methods
                                ByteBuffer bb = nioBuffers[nioBufIndex];
                                bb.position(bb.position() + (int) localWrittenBytes);
                                break;
                            } else { // readable == writtenBytes
                                in.remove();
                                break;
                            }
                        }
                    } else {
                        // Release all buffers
                        for (int i = msgCount; i > 0; i --) {
                            in.remove();
                        }
                    }
                    // try again as a ChannelFuture may be notified in the meantime and triggered another flush
                    continue;
                }
            }

            Object msg = in.current();
            if (msg instanceof ByteBuf) {
                ByteBuf buf = (ByteBuf) msg;
                int readableBytes = buf.readableBytes();
                if (readableBytes == 0) {
                    in.remove();
                    continue;
                }

                int expected = buf.readableBytes();
                int localFlushedAmount = doWriteBytes(buf, expected);
                in.progress(localFlushedAmount);
                if (localFlushedAmount < expected) {
                    setEpollOut();
                    break;
                }
                if (!buf.isReadable()) {
                    in.remove();
                }

            } else if (msg instanceof DefaultFileRegion) {
                DefaultFileRegion region = (DefaultFileRegion) msg;

                long expected = region.count() - region.position();
                long localFlushedAmount = doWriteFileRegion(region, expected);
                in.progress(localFlushedAmount);

                if (localFlushedAmount < expected) {
                    setEpollOut();
                    break;
                }

                if (region.transfered() >= region.count()) {
                    in.remove();
                }
            } else {
                throw new UnsupportedOperationException("unsupported message type: " + StringUtil.simpleClassName(msg));
            }
        }
    }

    @Override
    public EpollSocketChannelConfig config() {
        return config;
    }

    @Override
    public boolean isInputShutdown() {
        return inputShutdown;
    }

    @Override
    public boolean isOutputShutdown() {
        return outputShutdown || !isActive();
    }

    @Override
    public ChannelFuture shutdownOutput() {
        return shutdownOutput(newPromise());
    }

    @Override
    public ChannelFuture shutdownOutput(final ChannelPromise promise) {
        EventLoop loop = eventLoop();
        if (loop.inEventLoop()) {
            try {
                Native.shutdown(fd, false, true);
                outputShutdown = true;
                promise.setSuccess();
            } catch (Throwable t) {
                promise.setFailure(t);
            }
        } else {
            loop.execute(new Runnable() {
                @Override
                public void run() {
                    shutdownOutput(promise);
                }
            });
        }
        return promise;
    }

    @Override
    public ServerSocketChannel parent() {
        return (ServerSocketChannel) super.parent();
    }

    final class NativeUnsafeImpl extends NativeUnsafe {
        private RecvByteBufAllocator.Handle allocHandle;

        @Override
        public void write(Object msg, ChannelPromise promise) {
            if (msg instanceof ByteBuf) {
                ByteBuf buf = (ByteBuf) msg;
                if (!buf.isDirect()) {
                    // We can only handle direct buffers so we need to copy if a non direct is
                    // passed to write.
                    int readable = buf.readableBytes();
                    ByteBuf dst = alloc().directBuffer(readable);
                    dst.writeBytes(buf, buf.readerIndex(), readable);

                    buf.release();
                    msg = dst;
                }
            }
            super.write(msg, promise);
        }

        private void closeOnRead(ChannelPipeline pipeline) {
            inputShutdown = true;
            if (isOpen()) {
                if (Boolean.TRUE.equals(config().getOption(ChannelOption.ALLOW_HALF_CLOSURE))) {
                    clearEpollIn();
                    pipeline.fireUserEventTriggered(ChannelInputShutdownEvent.INSTANCE);
                } else {
                    close(voidPromise());
                }
            }
        }

        private void handleReadException(ChannelPipeline pipeline, ByteBuf byteBuf, Throwable cause, boolean close) {
            if (byteBuf != null) {
                if (byteBuf.isReadable()) {
                    pipeline.fireChannelRead(byteBuf);
                } else {
                    byteBuf.release();
                }
            }
            pipeline.fireChannelReadComplete();
            pipeline.fireExceptionCaught(cause);
            if (close || cause instanceof IOException) {
                closeOnRead(pipeline);
            }
        }

        @Override
        public void connect(
                final SocketAddress remoteAddress, final SocketAddress localAddress, final ChannelPromise promise) {
            if (!ensureOpen(promise)) {
                return;
            }

            try {
                if (connectPromise != null) {
                    throw new IllegalStateException("connection attempt already made");
                }

                boolean wasActive = isActive();
                if (doConnect((InetSocketAddress) remoteAddress, (InetSocketAddress) localAddress)) {
                    active = true;
                    promise.setSuccess();
                    if (!wasActive && isActive()) {
                        pipeline().fireChannelActive();
                    }
                } else {
                    connectPromise = promise;
                    requestedRemoteAddress = remoteAddress;

                    // Schedule connect timeout.
                    int connectTimeoutMillis = config().getConnectTimeoutMillis();
                    if (connectTimeoutMillis > 0) {
                        connectTimeoutFuture = eventLoop().schedule(new Runnable() {
                            @Override
                            public void run() {
                                ChannelPromise connectPromise = EpollSocketChannel.this.connectPromise;
                                ConnectTimeoutException cause =
                                        new ConnectTimeoutException("connection timed out: " + remoteAddress);
                                if (connectPromise != null && connectPromise.tryFailure(cause)) {
                                    close(voidPromise());
                                }
                            }
                        }, connectTimeoutMillis, TimeUnit.MILLISECONDS);
                    }

                    promise.addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (future.isCancelled()) {
                                if (connectTimeoutFuture != null) {
                                    connectTimeoutFuture.cancel(false);
                                }
                                connectPromise = null;
                                close(voidPromise());
                            }
                        }
                    });
                }
            } catch (Throwable t) {
                if (t instanceof ConnectException) {
                    Throwable newT = new ConnectException(t.getMessage() + ": " + remoteAddress);
                    newT.setStackTrace(t.getStackTrace());
                    t = newT;
                }
                closeIfClosed();
                promise.tryFailure(t);
            }
        }

        @Override
        void epollOutReady() {
            if (connectPromise != null) {
                // pending connect which is now complete so handle it.
                finishConnect();
            } else {
                super.epollOutReady();
            }
        }

        private void finishConnect() {
            // Note this method is invoked by the event loop only if the connection attempt was
            // neither cancelled nor timed out.

            assert eventLoop().inEventLoop();
            assert connectPromise != null;

            try {
                boolean wasActive = isActive();
                doFinishConnect();
                active = true;
                connectPromise.setSuccess();
                if (!wasActive && isActive()) {
                    pipeline().fireChannelActive();
                }
            } catch (Throwable t) {
                if (t instanceof ConnectException) {
                    Throwable newT = new ConnectException(t.getMessage() + ": " + requestedRemoteAddress);
                    newT.setStackTrace(t.getStackTrace());
                    t = newT;
                }

                connectPromise.setFailure(t);
                closeIfClosed();
            } finally {
                connectTimeoutFuture.cancel(false);
                connectPromise = null;
            }
        }

        /**
         * Connect to the remote peer
         */
        private boolean doConnect(InetSocketAddress remoteAddress, InetSocketAddress localAddress) throws Exception {
            if (localAddress != null) {
                Native.bind(fd, localAddress.getAddress(), localAddress.getPort());
            }

            boolean success = false;
            try {
                boolean connected = Native.connect(fd, remoteAddress.getAddress(),
                        remoteAddress.getPort());
                if (!connected) {
                    setEpollOut();
                }
                success = true;
                return connected;
            } finally {
                if (!success) {
                    doClose();
                }
            }
        }

        /**
         * Finish the connect
         */
        private void doFinishConnect() throws Exception {
            Native.finishConnect(fd);
            clearEpollOut();
        }

        @Override
        void epollInReady() {
            final ChannelConfig config = config();
            final ChannelPipeline pipeline = pipeline();
            final ByteBufAllocator allocator = config.getAllocator();
            RecvByteBufAllocator.Handle allocHandle = this.allocHandle;
            if (allocHandle == null) {
                this.allocHandle = allocHandle = config.getRecvByteBufAllocator().newHandle();
            }
            if (!config.isAutoRead()) {
                clearEpollIn();
            }

            ByteBuf byteBuf = null;
            boolean close = false;
            try {
                int byteBufCapacity = allocHandle.guess();
                int totalReadAmount = 0;
                for (;;) {
                    // we use a direct buffer here as the native implementations only be able
                    // to handle direct buffers.
                    byteBuf = allocator.directBuffer(byteBufCapacity);
                    int writable = byteBuf.writableBytes();
                    int localReadAmount = doReadBytes(byteBuf);
                    if (localReadAmount <= 0) {
                        // not was read release the buffer
                        byteBuf.release();
                        close = localReadAmount < 0;
                        break;
                    }
                    pipeline.fireChannelRead(byteBuf);
                    byteBuf = null;

                    totalReadAmount += localReadAmount;
                    if (localReadAmount < writable) {
                        // Read less than what the buffer can hold,
                        // which might mean we drained the recv buffer completely.
                        break;
                    }
                }

                pipeline.fireChannelReadComplete();
                allocHandle.record(totalReadAmount);

                if (close) {
                    closeOnRead(pipeline);
                    close = false;
                }
            } catch (Throwable t) {
                handleReadException(pipeline, byteBuf, t, close);
            }
        }
    }
}
