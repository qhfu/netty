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
package io.netty.jni.buffer;

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;

/**
 * Uses native calls (JNI) to allocate / release direct ByteBufs for max performance.
 */
public final class UnpooledNativeByteBufAllocator extends AbstractByteBufAllocator {

    /**
     * Default instance
     */
    public static final UnpooledNativeByteBufAllocator DEFAULT =
            new UnpooledNativeByteBufAllocator(PlatformDependent.directBufferPreferred());

    public UnpooledNativeByteBufAllocator(boolean preferDirect) {
        super(preferDirect);
    }

    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
        if (PlatformDependent.hasUnsafe()) {
            return new UnpooledUnsafeNativeDirectByteBuf(this, initialCapacity, maxCapacity);
        } else {
            return new UnpooledNativeDirectByteBuf(this, initialCapacity, maxCapacity);
        }
    }

    @Override
    protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
        return new UnpooledNativeHeapByteBuf(this, initialCapacity, maxCapacity);
    }

    @Override
    public boolean isDirectBufferPooled() {
        return false;
    }
}
