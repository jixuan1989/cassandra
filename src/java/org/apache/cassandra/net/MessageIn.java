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

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.FileUtils;
/**
 * 传入的消息。
 * @author hxd
 *
 * @param <T>
 */
public class MessageIn<T>
{
    public final InetAddress from;
    /**
     * 具体消息
     */
    public final T payload;
    public final Map<String, byte[]> parameters;
    public final MessagingService.Verb verb;
    public final int version;

    private MessageIn(InetAddress from, T payload, Map<String, byte[]> parameters, MessagingService.Verb verb, int version)
    {
        this.from = from;
        this.payload = payload;
        this.parameters = parameters;
        this.verb = verb;
        this.version = version;
    }

    public static <T> MessageIn<T> create(InetAddress from, T payload, Map<String, byte[]> parameters, MessagingService.Verb verb, int version)
    {
        return new MessageIn<T>(from, payload, parameters, verb, version);
    }
    /**
     * 从流中反序列化出信息并封装成MessageIn
     * @param in 输入流
     * @param version 反序列化payload以及创建messageIn时使用
     * @param id   对于有callback需要调用的verb，可根据该id获得其callback函数
     * @return
     * @throws IOException
     */
    public static <T2> MessageIn<T2> read(DataInput in, int version, String id) throws IOException
    {
        InetAddress from = CompactEndpointSerializationHelper.deserialize(in);//从in流中首先序列化出ip地址（ip地址位数+ip地址内容）

        MessagingService.Verb verb = MessagingService.Verb.values()[in.readInt()];//读出动作的编号（enum自带编号）
        int parameterCount = in.readInt();//读出参数个数
        Map<String, byte[]> parameters;
        if (parameterCount == 0)
        {
            parameters = Collections.emptyMap();
        }
        else//依次读出参数名（readUTF）-参数值（大小+byte[])
        {
            ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builder();
            for (int i = 0; i < parameterCount; i++)
            {
                String key = in.readUTF();
                byte[] value = new byte[in.readInt()];
                in.readFully(value);
                builder.put(key, value);
            }
            parameters = builder.build();
        }

        int payloadSize = in.readInt();//读出payload长度
        IVersionedSerializer<T2> serializer = (IVersionedSerializer<T2>) MessagingService.verbSerializers.get(verb);
        if (serializer instanceof MessagingService.CallbackDeterminedSerializer)//判断动作所属的序列化器是不是CallbackDeterminedSerializer（两种动作是这样子的，MessageService.verbSerializers中进行了定义）
        {
            CallbackInfo callback = MessagingService.instance().getRegisteredCallback(id);//从expiringMap中得到该回调函数
            if (callback == null)
            {
                // reply for expired callback.  we'll have to skip it.
                FileUtils.skipBytesFully(in, payloadSize);
                return null;
            }
            serializer = (IVersionedSerializer<T2>) callback.serializer;
        }
        if (payloadSize == 0 || serializer == null)
            return create(from, null, parameters, verb, version);
        T2 payload = serializer.deserialize(in, version);//从流中反序列化出真正的payload
        return MessageIn.create(from, payload, parameters, verb, version);
    }

    public Stage getMessageType()
    {
        return MessagingService.verbStages.get(verb);
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getTimeout(verb);
    }

    public String toString()
    {
        StringBuilder sbuf = new StringBuilder("");
        sbuf.append("FROM:").append(from).append(" TYPE:").append(getMessageType()).append(" VERB:").append(verb);
        return sbuf.toString();
    }
}
