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
package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.util.concurrent.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;

/**
 * 周期性的CommitLog的执行任务类，主要包含两部分工作
 * <br>1.启动了一个内部的常驻的CommitLog写入操作的调度线程，每100ms调度一次
 * <br>2.commitLog的同步线程的控制线程，每隔commitlog_sync_period_in_ms(配置文件中定义)时常将内存中的commitLog刷入磁盘的文件中
 */
class PeriodicCommitLogExecutorService implements ICommitLogExecutorService
{
    private final BlockingQueue<Runnable> queue; //commitLog写入线程以及同步线程的的执行队列
    protected volatile long completedTaskCount = 0;
    private final Thread appendingThread;
    private volatile boolean run = true;

    public PeriodicCommitLogExecutorService(final CommitLog commitLog)
    {
        queue = new LinkedBlockingQueue<Runnable>(DatabaseDescriptor.getCommitLogPeriodicQueueSize());
        //作为后台常驻线程运行，每100ms执行一次
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                while (run)
                {
                    Runnable r = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (r == null)
                        continue;
                    r.run();
                    completedTaskCount++;
                }
                commitLog.sync();
            }
        };
        appendingThread = new Thread(runnable, "COMMIT-LOG-WRITER");
        appendingThread.start();

        /**
         * commitLog的同步线程
         */
        final Callable syncer = new Callable()
        {
            public Object call() throws Exception
            {
                commitLog.sync();
                return null;
            }
        };

        /**
         * commitLog的同步线程的控制线程，每隔commitlog_sync_period_in_ms毫秒向线程队列中提交一次commitLog的同步任务
         */
        new Thread(new Runnable()
        {
            public void run()
            {
                while (run)
                {
                    try
                    {
                        submit(syncer).get();
                        Thread.sleep(DatabaseDescriptor.getCommitLogSyncPeriod());
                    }
                    catch (InterruptedException e)
                    {
                        throw new AssertionError(e);
                    }
                    catch (ExecutionException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            }
        }, "PERIODIC-COMMIT-LOG-SYNCER").start();

    }

    /**
     * 将该操作加入到周期性CommitLog服务的执行队列中
     * @param adder 要加入的CommitLog写入操作
     */
    public void add(CommitLog.LogRecordAdder adder)
    {
        try
        {
            queue.put(adder);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public <T> Future<T> submit(Callable<T> task)
    {
        FutureTask<T> ft = new FutureTask<T>(task);
        try
        {
            queue.put(ft);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        return ft;
    }

    public void shutdown()
    {
        new Thread(new WrappedRunnable()
        {
            public void runMayThrow() throws InterruptedException, IOException
            {
                while (!queue.isEmpty())
                    Thread.sleep(100);
                run = false;
                appendingThread.join();
            }
        }, "Commitlog Shutdown").start();
    }

    public void awaitTermination() throws InterruptedException
    {
        appendingThread.join();
    }

    public long getPendingTasks()
    {
        return queue.size();
    }

    public long getCompletedTasks()
    {
        return completedTaskCount;
    }

}