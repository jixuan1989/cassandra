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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.UUID;

public abstract class TypeSizes
{
    public static final TypeSizes NATIVE = new NativeDBTypeSizes();
    public static final TypeSizes VINT = new VIntEncodedTypeSizes();

    private static final int BOOL_SIZE = 1;
    private static final int SHORT_SIZE = 2;
    private static final int INT_SIZE = 4;
    private static final int LONG_SIZE = 8;
    private static final int UUID_SIZE = 16;

    public abstract int sizeof(boolean value);
    public abstract int sizeof(short value);
    public abstract int sizeof(int value);
    public abstract int sizeof(long value);
    public abstract int sizeof(UUID value);

    /** assumes UTF8 */
    public int sizeof(String value)
    {
        int length = encodedUTF8Length(value);
        assert length <= Short.MAX_VALUE;
        return sizeof((short) length) + length;
    }

    public static int encodedUTF8Length(String st)
    {
        int strlen = st.length();
        int utflen = 0;
        for (int i = 0; i < strlen; i++)
        {
            int c = st.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F))
                utflen++;
            else if (c > 0x07FF)
                utflen += 3;
            else
                utflen += 2;
        }
        return utflen;
    }
    /**返回value中的剩余长度，以及该长度这个short int型的长度 之和**/
    public int sizeofWithShortLength(ByteBuffer value)
    {
        return sizeof((short) value.remaining()) + value.remaining();
    }
    /**返回value中的剩余长度，以及该长度这个int型的长度 之和**/
    public int sizeofWithLength(ByteBuffer value)
    {
        return sizeof(value.remaining()) + value.remaining();
    }
    /**返回各种数据类型标准的大小**/
    public static class NativeDBTypeSizes extends TypeSizes
    {
        public int sizeof(boolean value)
        {
            return BOOL_SIZE;
        }

        public int sizeof(short value)
        {
            return SHORT_SIZE;
        }

        public int sizeof(int value)
        {
            return INT_SIZE;
        }

        public int sizeof(long value)
        {
            return LONG_SIZE;
        }

        public int sizeof(UUID value)
        {
            return UUID_SIZE;
        }
    }
/**尚未搞清楚是如何encode的。 **/
    public static class VIntEncodedTypeSizes extends TypeSizes
    {
        private static final int BOOL_SIZE = 1;

        public int sizeofVInt(long i)
        {
            if (i >= -112 && i <= 127)//112是一个很神奇的数。不明觉厉 。其二进制形式为01110000
                return 1;

            int size = 0;
            int len = -112;
            if (i < 0)
            {
                i ^= -1L; // take one's complement' 得到反码（正数不变，负数变正数然后-1）
                len = -120;
            }
            long tmp = i;
            while (tmp != 0)
            {
                tmp = tmp >> 8;
                len--;
            }
            size++;//到这一步为止，size始终等于1嘛。。。。
            len = (len < -120) ? -(len + 120) : -(len + 112);//当i为负数是，走前面，i为正数走后面
            size += len;
            return size;
        }

        public int sizeof(long i)
        {
            return sizeofVInt(i);
        }

        public int sizeof(boolean i)
        {
            return BOOL_SIZE;
        }

        public int sizeof(short i)
        {
            return sizeofVInt(i);
        }

        public int sizeof(int i)
        {
            return sizeofVInt(i);
        }

        public int sizeof(UUID value)
        {
            return sizeofVInt(value.getMostSignificantBits()) + sizeofVInt(value.getLeastSignificantBits());
        }
    }
}
