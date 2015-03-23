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
package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Objects;

import org.apache.cassandra.db.CounterColumn;
import org.apache.cassandra.db.SystemTable;

public class CounterId implements Comparable<CounterId>
{
    private static final Logger logger = LoggerFactory.getLogger(CounterId.class);

    public static final int LENGTH = 16; // we assume a fixed length size for all CounterIds

    // Lazy holder because this opens the system table and we want to avoid
    // having this triggered during class initialization
    private static class LocalIds
    {
        static final LocalCounterIdHistory instance = new LocalCounterIdHistory();
    }

    private final ByteBuffer id;

    private static LocalCounterIdHistory localIds()
    {
        return LocalIds.instance;
    }
    /**返回本地记录的 最新的那个id**/
    public static CounterId getLocalId()
    {
        return localIds().current.get();
    }

    /**
     * Renew the local counter id.
     * To use only when this strictly necessary, as using this will make all
     * counter context grow with time.
     */
    public static void renewLocalId()
    {
        renewLocalId(FBUtilities.timestampMicros());
    }
    /**生成新的id并写入系统表，将原有id放入历史中。注意该方法有synchronized关键字*/
    public static synchronized void renewLocalId(long now)
    {
        localIds().renewCurrent(now);
    }

    /**
     * Return the list of old local counter id of this node.
     * It is guaranteed that the returned list is sorted by growing counter id
     * (and hence the first item will be the oldest counter id for this host)
     */
    public static List<CounterIdRecord> getOldLocalCounterIds()
    {
        return localIds().olds;
    }

    /**
     * Function for test purposes, do not use otherwise.
     * Pack an int in a valid CounterId so that the resulting ids respects the
     * numerical ordering. Used for creating handcrafted but easy to
     * understand contexts in unit tests (see CounterContextTest).
     */
    public static CounterId fromInt(int n)
    {
        long lowBits = 0xC000000000000000L | n;
        return new CounterId(ByteBuffer.allocate(16).putLong(0, 0).putLong(8, lowBits));
    }

    /*
     * For performance reasons, this function interns the provided ByteBuffer.
     */
    public static CounterId wrap(ByteBuffer id)
    {
        return new CounterId(id);
    }

    public static CounterId wrap(ByteBuffer bb, int offset)
    {
        ByteBuffer dup = bb.duplicate();
        dup.position(offset);
        dup.limit(dup.position() + LENGTH);
        return wrap(dup);
    }

    private CounterId(ByteBuffer id)
    {
        if (id.remaining() != LENGTH)
            throw new IllegalArgumentException("A CounterId representation is exactly " + LENGTH + " bytes");

        this.id = id;
    }
    /**按时间uuid生成*/
    public static CounterId generate()
    {
        return new CounterId(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes()));
    }

    /*
     * For performance reasons, this function returns a reference to the internal ByteBuffer. Clients not modify the
     * result of this function.
     */
    public ByteBuffer bytes()
    {
        return id;
    }

    public boolean isLocalId()
    {
        return equals(getLocalId());
    }

    public int compareTo(CounterId o)
    {
        return ByteBufferUtil.compareSubArrays(id, id.position(), o.id, o.id.position(), CounterId.LENGTH);
    }

    @Override
    public String toString()
    {
        return UUIDGen.getUUID(id).toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        CounterId otherId = (CounterId)o;
        return id.equals(otherId.id);
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }
    /**这个类只会renew一次。 并且仅在给定的column的上下文中包含该类构造时时的counter id才生成*/
    public static class OneShotRenewer
    {
        private boolean renewed;
        private final CounterId initialId;

        public OneShotRenewer()
        {
            renewed = false;
            initialId = getLocalId();
        }
        /**如果该类没有真的renew过，并且给定的column的上下文中包含该类构造时时的counter id， 则生成一个*/
        public void maybeRenew(CounterColumn column)
        {
            if (!renewed && column.hasCounterId(initialId))
            {
                renewLocalId();
                renewed = true;
            }
        }
    }
    /**从系统表中读取counter，并保存在类中。也可以初始化系统表的counter值。*/
    private static class LocalCounterIdHistory
    {
        private final AtomicReference<CounterId> current;
        private final List<CounterIdRecord> olds;

        LocalCounterIdHistory()
        {
            CounterId id = SystemTable.getCurrentLocalCounterId();
            if (id == null)
            {
                // no recorded local counter id, generating a new one and saving it
                id = generate();
                logger.info("No saved local counter id, using newly generated: {}", id);
                SystemTable.writeCurrentLocalCounterId(null, id, FBUtilities.timestampMicros());
                current = new AtomicReference<CounterId>(id);
                olds = new CopyOnWriteArrayList<CounterIdRecord>();
            }
            else
            {
                logger.info("Saved local counter id: {}", id);
                current = new AtomicReference<CounterId>(id);
                olds = new CopyOnWriteArrayList<CounterIdRecord>(SystemTable.getOldLocalCounterIds());
            }
        }
        /**生成新的id并写入系统表，将原有id放入历史中。注意该方法有synchronized关键字*/
        synchronized void renewCurrent(long now)
        {
            CounterId newCounterId = generate();
            CounterId old = current.get();
            SystemTable.writeCurrentLocalCounterId(old, newCounterId, now);
            current.set(newCounterId);
            olds.add(new CounterIdRecord(old, now));
        }
    }

    public static class CounterIdRecord
    {
        public final CounterId id;
        public final long timestamp;

        public CounterIdRecord(CounterId id, long timestamp)
        {
            this.id = id;
            this.timestamp = timestamp;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            CounterIdRecord otherRecord = (CounterIdRecord)o;
            return id.equals(otherRecord.id) && timestamp == otherRecord.timestamp;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(id, timestamp);
        }

        public String toString()
        {
            return String.format("(%s, %d)", id.toString(), timestamp);
        }
    }
}
