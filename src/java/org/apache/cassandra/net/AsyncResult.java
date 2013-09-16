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
package org.apache.cassandra.net;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class AsyncResult<T> implements IAsyncResult<T>
{
    private T result;//用来存储结果
    private final AtomicBoolean done = new AtomicBoolean(false);
    private final Lock lock = new ReentrantLock();
    private final Condition condition;
    private final long startTime;
    private InetAddress from;

    public AsyncResult()
    {
        condition = lock.newCondition();
        startTime = System.currentTimeMillis();
    }
/**
 * 似乎就做了看在规定时间内能否拿到锁（也就是看规定的时间内有没有调用过result（）函数）
 */
    public T get(long timeout, TimeUnit tu) throws TimeoutException
    {
        lock.lock();
        try
        {
            boolean bVal = true;
            try
            {
                if (!done.get())
                {
                    timeout = TimeUnit.MILLISECONDS.convert(timeout, tu);
                    long overall_timeout = timeout - (System.currentTimeMillis() - startTime);
                    bVal = overall_timeout > 0 && condition.await(overall_timeout, TimeUnit.MILLISECONDS);
                }
            }
            catch (InterruptedException ex)
            {
                throw new AssertionError(ex);
            }

            if (!bVal && !done.get())
            {
                throw new TimeoutException("Operation timed out.");
            }
        }
        finally
        {
            lock.unlock();
        }
        return result;
    }
/**
 * 若从没调用过result（） 则获取response的from和result属性，并设置为done，然后通知get方法中的wait()函数。
 *  否则直接返回。
 */
    public void result(MessageIn<T> response)
    {
        try
        {
            lock.lock();
            if (!done.get())
            {
                from = response.from;
                result = response.payload;
                done.set(true);
                condition.signal();
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }

    public InetAddress getFrom()
    {
        return from;
    }
}
