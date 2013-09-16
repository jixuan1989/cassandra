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
package org.apache.cassandra.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class JMXConfigurableThreadPoolExecutor extends JMXEnabledThreadPoolExecutor implements JMXConfigurableThreadPoolExecutorMBean
{
	/**
	 * 创建一个固定大小的线程池，允许coreThread被回收，绑定一个钩子：如果队列满了，被拒绝，则不断充实。
	 * <br>启动所有核心线程，使其处于等待工作的空闲状态。
	 * <br> 然后注册该Mbean，启动度量各种线程状态的度量器
	 * @param corePoolSize
	 * @param keepAliveTime
	 * @param unit
	 * @param workQueue
	 * @param threadFactory
	 * @param jmxPath
	 */
    public JMXConfigurableThreadPoolExecutor(int corePoolSize,
                                             long keepAliveTime,
                                             TimeUnit unit,
                                             BlockingQueue<Runnable> workQueue,
                                             NamedThreadFactory threadFactory,
                                             String jmxPath)
    {
        super(corePoolSize, keepAliveTime, unit, workQueue, threadFactory, jmxPath);
    }

}
