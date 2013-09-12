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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.xerial.snappy.SnappyOutputStream;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
/**
 * 该类是一个线程类。维护了两个message的排队列表：active和backlog。<br>
 * 线程的任务是：从active中读取消息，写出去，如果actvie中读完的话，就从backlog中读。（会使backlog变成active，active变成backlog）<br>
 * <hr>
 * 该类还做了其他的事情：
 * <br>1、enquence():将消息加入backlog队列中（期间会drop掉已经超时的消息）。
 * <br>2、connect():根据pool中的endpoint地址创建连接，并比较版本号，更新MessageService中的versions里对应ip的版本号。
 * <hr>
 * 还有一些要注意的是：该类事实上会多次进行connect，也会多次disconnect。但是disconnect并不意味着线程的终结和消息不再发出，而是根据isStopped字段决定是否继续run的。<br>
 * isStopped是在pool的close或者reset的时候才会可能被修改的（本类中的closeSocket方法可选是否设置stopped）。
 * @author hxd注释
 *
 */
public class OutboundTcpConnection extends Thread
{
    private static final Logger logger = LoggerFactory.getLogger(OutboundTcpConnection.class);

    private static final MessageOut CLOSE_SENTINEL = new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE);
    private volatile boolean isStopped = false;

    private static final int OPEN_RETRY_DELAY = 100; // ms between retries

    // sending thread reads from "active" (one of queue1, queue2) until it is empty.
    // then it swaps it with "backlog."
    private volatile BlockingQueue<QueuedMessage> backlog = new LinkedBlockingQueue<QueuedMessage>();
    private volatile BlockingQueue<QueuedMessage> active = new LinkedBlockingQueue<QueuedMessage>();

    private final OutboundTcpConnectionPool poolReference;

    private DataOutputStream out;
    private Socket socket;
    private volatile long completed;
    private final AtomicLong dropped = new AtomicLong();
    private int targetVersion;

    public OutboundTcpConnection(OutboundTcpConnectionPool pool)
    {
        super("WRITE-" + pool.endPoint());
        this.poolReference = pool;
    }

    /**
     * 查看targetHost与本机是否在同一个DC下。
     * @param targetHost
     * @return
     */
    private static boolean isLocalDC(InetAddress targetHost)
    {
        String remoteDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(targetHost);
        String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
        return remoteDC.equals(localDC);
    }
/**
 * 将message加入到backlog队列中，首先调用{@link expireMessages() }。然后在加入backlog队列。<br>
 * 注意，该方法被messageService的sendOneWay，以及本类的[soft]closeSocket调用了。
 * @param message
 * @param id
 */
    public void enqueue(MessageOut<?> message, String id)
    {
        expireMessages();//大意是看看依次查看backlog的消息是否超时，超时就扔了，遇到没超时的，就不再往下查看了（再看浪费时间，而且也不好从中间remove），个人感觉这样是为了降低backlog的长度。
        try
        {
            backlog.put(new QueuedMessage(message, id));
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }
/**
 * 清空队列里的message。然后加入一个close_sentinel消息到队列
 * @param destroyThread
 */
    void closeSocket(boolean destroyThread)
    {
        active.clear();
        backlog.clear();
        isStopped = destroyThread; // Exit loop to stop the thread
        enqueue(CLOSE_SENTINEL, null);
    }
/**
 * 不清空队列里的message，然后加入一个close_sentinel消息到队列
 */
    void softCloseSocket()
    {
        enqueue(CLOSE_SENTINEL, null);
    }

    public int getTargetVersion()
    {
        return targetVersion;
    }
/**
 * 不断地从active中取出消息<br>
 * 如果active中没了，就从backlog中取（阻塞的），然后交换backlog和active。<br>
 * 若消息是close_sentinel，就关闭连接。注意，并不一定退出run。而是看isStopped,继续下一次循环或者跳出循环。<br>
 * 如果消息已经超时，就drop掉。否则：若连接开着的或者重新连接上了，就发送消息；否则active.clear（这么狠？）<br>
 * 
 */
    public void run()
    {
        while (true)
        {
            QueuedMessage qm = active.poll();
            if (qm == null)
            {
                // exhausted the active queue.  switch to backlog, once there's something to process there
                try
                {
                    qm = backlog.take();
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }

                BlockingQueue<QueuedMessage> tmp = backlog;
                backlog = active;
                active = tmp;
            }

            MessageOut<?> m = qm.message;
            if (m == CLOSE_SENTINEL)
            {
                disconnect();
                if (isStopped)
                    break;
                continue;
            }
            if (qm.timestamp < System.currentTimeMillis() - m.getTimeout())
                dropped.incrementAndGet();
            else if (socket != null || connect()){
            	logger.trace("write a message:{}",qm);
            	writeConnected(qm);
            }
            else
                // clear out the queue, else gossip messages back up.
                active.clear();
        }
    }
/**
 * 得到要做的数量，是active和backlog的和
 * @return
 */
    public int getPendingMessages()
    {
        return active.size() + backlog.size();
    }
/**
 * 得到完成的数量
 * @return
 */
    public long getCompletedMesssages()
    {
        return completed;
    }
/**
 * 得到drop掉的数量
 * @return
 */
    public long getDroppedMessages()
    {
        return dropped.get();
    }

    /**
     * 配置文件设置了全压缩，或者（设置了dc间压缩，且pool.endPoint与本机不是同一个DC） 则返回true
     * @return
     */
    private boolean shouldCompressConnection()
    {
        // assumes version >= 1.2
        return DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.all
               || (DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.dc && !isLocalDC(poolReference.endPoint()));
    }
/**
 * 首先获取消息中的traceSession,然后记录到trace中并有可能移除tracession（？）<br>
 * 发送消息并将完成的计数器+1。注意，当且仅当当前活动队列为空时，才对输出流flush..<br>
 * 如果发送中出现IOException，且该消息是重要消息（require retry），则封装成retryMessage并重新放入backlog队列中<br>
 * @param qm
 */
    private void writeConnected(QueuedMessage qm)
    {
        try
        {
            byte[] sessionBytes = qm.message.parameters.get(Tracing.TRACE_HEADER);
            if (sessionBytes != null)
            {
                UUID sessionId = UUIDGen.getUUID(ByteBuffer.wrap(sessionBytes));
                TraceState state = Tracing.instance().get(sessionId);
                state.trace("Sending message to {}", poolReference.endPoint());
                Tracing.instance().stopIfNonLocal(state);
            }

            write(qm.message, qm.id, qm.timestamp, out, targetVersion);
            completed++;
            if (active.peek() == null)
            {
                out.flush();
            }
        }
        catch (Exception e)
        {
            disconnect();
            if (e instanceof IOException)
            {
                if (logger.isDebugEnabled())
                    logger.debug("error writing to " + poolReference.endPoint(), e);

                // if the message was important, such as a repair acknowledgement, put it back on the queue
                // to retry after re-connecting.  See CASSANDRA-5393
                if (e instanceof SocketException && qm.shouldRetry())
                {
                    try
                    {
                        backlog.put(new RetriedQueuedMessage(qm));
                    }
                    catch (InterruptedException e1)
                    {
                        throw new AssertionError(e1);
                    }
                }
            }
            else
            {
                // Non IO exceptions are likely a programming error so let's not silence them
                logger.error("error writing to " + poolReference.endPoint(), e);
            }
        }
    }
/**
 * 将消息序列化写出，
 * TODO 具体协议暂略
 * @param message
 * @param id
 * @param timestamp
 * @param out
 * @param version
 * @throws IOException
 */
    public static void write(MessageOut message, String id, long timestamp, DataOutputStream out, int version) throws IOException
    {
        out.writeInt(MessagingService.PROTOCOL_MAGIC);
        if (version < MessagingService.VERSION_12)
        {
            writeHeader(out, version, false);
            // 0.8 included a total message size int.  1.0 doesn't need it but expects it to be there.
            out.writeInt(-1);
        }

        out.writeUTF(id);
        if (version >= MessagingService.VERSION_12)
        {
            // int cast cuts off the high-order half of the timestamp, which we can assume remains
            // the same between now and when the recipient reconstructs it.
            out.writeInt((int) timestamp);
        }
        message.serialize(out, version);
    }
/**
 * TODO 
 * @param out
 * @param version
 * @param compressionEnabled
 * @throws IOException
 */
    private static void writeHeader(DataOutputStream out, int version, boolean compressionEnabled) throws IOException
    {
        // 2 bits: unused.  used to be "serializer type," which was always Binary
        // 1 bit: compression
        // 1 bit: streaming mode
        // 3 bits: unused
        // 8 bits: version
        // 15 bits: unused
        int header = 0;
        if (compressionEnabled)
            header |= 4;
        header |= (version << 8);
        out.writeInt(header);
    }
/**
 * 关闭socket
 */
    private void disconnect()
    {
        if (socket != null)
        {
            try
            {
                socket.close();
            }
            catch (IOException e)
            {
                if (logger.isTraceEnabled())
                    logger.trace("exception closing connection to " + poolReference.endPoint(), e);
            }
            out = null;
            socket = null;
        }
    }
/**
 * 从messageService中获取endPoint的version。<br>
 * 然后从pool中获取一个新socket，根据与endPoint的位置决定是否使用tcpNoDelay算法，<br>
 * 接下来进行一次通信，将获取的版本号发送过去，并获得反馈的版本号，进行对比，若前者新，则更新messageService中的version记录并关闭连接，返回fasle。 若后者新，则更新messageService中version记录，并添加关闭消息到队列中。<br>
 * 然后告知对方自己的版本号，之后根据配置文件中是否需要compress，若需要，则将out流用SnappyOutputStream封装。<br>
 * 以上过程如果出问题，就sleep 100ms 循环重试。<br>
 * @return
 */
    private boolean connect()
    {
        if (logger.isDebugEnabled())
            logger.debug("attempting to connect to " + poolReference.endPoint());

        targetVersion = MessagingService.instance().getVersion(poolReference.endPoint());

        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() < start + DatabaseDescriptor.getRpcTimeout())
        {
            try
            {
                socket = poolReference.newSocket();
                socket.setKeepAlive(true);
                if (isLocalDC(poolReference.endPoint()))
                {
                    socket.setTcpNoDelay(true);//Nagle算法用于对缓冲区内的一定数量的消息进行自动连接。该处理过程(称为Nagling)，通过减少必须发送的封包的数量，提高了网络应用 程序系统的效率。（Nagle虽然解决了小封包问题，但也导致了较高的不可预测的延迟，同时降低了吞吐量。）
                }
                else
                {
                    socket.setTcpNoDelay(DatabaseDescriptor.getInterDCTcpNoDelay());//对于多dc，看配置来决定是否开启tcpNoDelay
                }
                if (DatabaseDescriptor.getInternodeSendBufferSize() != null)
                {
                    try
                    {
                        socket.setSendBufferSize(DatabaseDescriptor.getInternodeSendBufferSize().intValue());
                    }
                    catch (SocketException se)
                    {
                        logger.warn("Failed to set send buffer size on internode socket.", se);
                    }
                }
                out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 4096));

                if (targetVersion >= MessagingService.VERSION_12)
                {
                    out.writeInt(MessagingService.PROTOCOL_MAGIC);
                    writeHeader(out, targetVersion, shouldCompressConnection());
                    out.flush();

                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    int maxTargetVersion = in.readInt();
                    if (targetVersion > maxTargetVersion)
                    {
                        logger.debug("Target max version is {}; will reconnect with that version", maxTargetVersion);
                        MessagingService.instance().setVersion(poolReference.endPoint(), maxTargetVersion);
                        disconnect();
                        return false;
                    }

                    if (targetVersion < maxTargetVersion && targetVersion < MessagingService.current_version)
                    {
                        logger.trace("Detected higher max version {} (using {}); will reconnect when queued messages are done",
                                     maxTargetVersion, targetVersion);
                        MessagingService.instance().setVersion(poolReference.endPoint(), Math.min(MessagingService.current_version, maxTargetVersion));
                        softCloseSocket();
                    }//这么看这段代码，如果current_version比maxTargetVersion小，则每次newSocket都会执行这段。

                    out.writeInt(MessagingService.current_version);
                    CompactEndpointSerializationHelper.serialize(FBUtilities.getBroadcastAddress(), out);
                    if (shouldCompressConnection())
                    {
                        out.flush();
                        logger.trace("Upgrading OutputStream to be compressed");
                        out = new DataOutputStream(new SnappyOutputStream(new BufferedOutputStream(socket.getOutputStream())));
                    }
                }

                return true;
            }
            catch (IOException e)
            {
                socket = null;
                if (logger.isTraceEnabled())
                    logger.trace("unable to connect to " + poolReference.endPoint(), e);
                try
                {
                    Thread.sleep(OPEN_RETRY_DELAY);
                }
                catch (InterruptedException e1)
                {
                    throw new AssertionError(e1);
                }
            }
        }
        return false;
    }
/**
 * 循环做以下事情<br>{<br>
 * 	如果backlog为空，啥都不说了。 如果第一个还没超时，那也啥都不说了。如果第一个已经超时了，将之从backlog中清除<br>}<br>
 * 	清空的时候，首先判断下和刚才判断的时候是否发生变化了，如果没变，扔了。。dropped计数+1。<br>
 *     如果变了，说明在active和backlog交换了（run函数中干的好事，发现active空了，就交换下active和backlog），那么这个消息应该是刚刚交换完后马上加进来的，为了不让他同其他的client（其他的client指的是？）竞争backlog的head锁（backlog是线程安全的），因此将之加入到active的末尾去，然后退出循环（可能是考虑这样的话，顶多就这一个消息在backlog中？还是故意跳出的？）。<br>
 * 该方法目前仅在enquence中调用了。<br>
 * TODO 感觉很有问题啊。会不会出现active.add之前 backlog和active又交换位置了呢。。此外还需要搞明白为何加入到active后就退出循环了。以及其他的client指的是什么？<br>
 * 
 */
    private void expireMessages()
    {
        while (true)
        {
            QueuedMessage qm = backlog.peek();
            if (qm == null || qm.timestamp >= System.currentTimeMillis() - qm.message.getTimeout())
                break;

            QueuedMessage qm2 = backlog.poll();
            if (qm2 != qm)
            {
                // sending thread switched queues.  add this entry (from the "new" backlog)
                // at the end of the active queue, which keeps it in the same position relative to the other entries
                // without having to contend with other clients for the head-of-backlog lock.
                if (qm2 != null)
                    active.add(qm2);
                break;
            }

            dropped.incrementAndGet();
        }
    }

    /** messages that have not been retried yet */
    private static class QueuedMessage
    {
        final MessageOut<?> message;
        final String id;
        final long timestamp;

        QueuedMessage(MessageOut<?> message, String id)
        {
            this.message = message;
            this.id = id;
            this.timestamp = System.currentTimeMillis();
        }

        boolean shouldRetry()
        {
            return !MessagingService.DROPPABLE_VERBS.contains(message.verb);
        }
    }

    private static class RetriedQueuedMessage extends QueuedMessage
    {
        RetriedQueuedMessage(QueuedMessage msg)
        {
            super(msg.message, msg.id);
        }

        boolean shouldRetry()
        {
            return false;
        }
    }
}
