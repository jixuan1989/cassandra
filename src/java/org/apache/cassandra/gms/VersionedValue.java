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
package org.apache.cassandra.gms;

import java.io.*;

import java.net.InetAddress;
import java.util.Collection;
import java.util.UUID;

import com.google.common.collect.Iterables;
import static com.google.common.base.Charsets.ISO_8859_1;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang.StringUtils;


/**
 * This abstraction represents the state associated with a particular node which an
 * application wants to make available to the rest of the nodes in the cluster.
 * Whenever a piece of state needs to be disseminated to the rest of cluster wrap
 * the state in an instance of <i>ApplicationState</i> and add it to the Gossiper.
 *
 * e.g. if we want to disseminate load information for node A do the following:
 *
 *      ApplicationState loadState = new ApplicationState(<string representation of load>);
 *      Gossiper.instance.addApplicationState("LOAD STATE", loadState);
 */

public class VersionedValue implements Comparable<VersionedValue>
{

    public static final IVersionedSerializer<VersionedValue> serializer = new VersionedValueSerializer();

    // this must be a char that cannot be present in any token
    public final static char DELIMITER = ',';
    public final static String DELIMITER_STR = new String(new char[] { DELIMITER });

    // values for ApplicationState.STATUS
    public final static String STATUS_BOOTSTRAPPING = "BOOT";
    public final static String STATUS_NORMAL = "NORMAL";
    public final static String STATUS_LEAVING = "LEAVING";
    public final static String STATUS_LEFT = "LEFT";
    public final static String STATUS_MOVING = "MOVING";
    public final static String STATUS_RELOCATING = "RELOCATING";

    public final static String REMOVING_TOKEN = "removing";
    public final static String REMOVED_TOKEN = "removed";

    public final static String HIBERNATE = "hibernate";

    // values for ApplicationState.REMOVAL_COORDINATOR
    public final static String REMOVAL_COORDINATOR = "REMOVER";

    public final int version;
    public final String value;

    private VersionedValue(String value, int version)
    {
        assert value != null;
        this.value = value;
        this.version = version;
    }
    /**
     * value=value. version=VersionGenerator.getNextVersion()
     * @param value
     */
    private VersionedValue(String value)
    {
        this(value, VersionGenerator.getNextVersion());
    }

    public int compareTo(VersionedValue value)
    {
        return this.version - value.version;
    }

    @Override
    public String toString()
    {
        return "Value(" + value + "," + version + ")";
    }
    /**
     * 将参数数组用分隔符(,)连接成一个字符串
     * @param args
     * @return
     */
    private static String versionString(String...args)
    {
        return StringUtils.join(args, VersionedValue.DELIMITER);
    }

    public static class VersionedValueFactory
    {
        final IPartitioner partitioner;

        public VersionedValueFactory(IPartitioner partitioner)
        {
            this.partitioner = partitioner;
        }
        /**
         * 返回一个versionValue对象， value="boot,*****" 其中****表示tokens的字符串编码形式
         * @param tokens
         * @return
         */
        public VersionedValue bootstrapping(Collection<Token> tokens)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_BOOTSTRAPPING,
                                                    makeTokenString(tokens)));
        }
        /**
         * 返回一个versionValue对象， value="normal,*****" 其中****表示tokens的字符串编码形式
         * @param tokens
         * @return
         */
        public VersionedValue normal(Collection<Token> tokens)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_NORMAL,
                                                    makeTokenString(tokens)));
        }
        /**
         * 将token列表变成一个字符串（通过partitioner来编译）
         * @param tokens
         * @return
         */
        @SuppressWarnings("unchecked")
		private String makeTokenString(Collection<Token> tokens)
        {
            return partitioner.getTokenFactory().toString(Iterables.get(tokens, 0));
        }

        public VersionedValue load(double load)
        {
            return new VersionedValue(String.valueOf(load));
        }

        public VersionedValue schema(UUID newVersion)
        {
            return new VersionedValue(newVersion.toString());
        }
        /**
         * 返回一个versionValue对象， value="leaving,*****" 其中****表示tokens的字符串编码形式
         * @param tokens
         * @return
         */
        public VersionedValue leaving(Collection<Token> tokens)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_LEAVING,
                    makeTokenString(tokens)));
        }
        /**
         * 返回一个versionValue对象， value="left,*****,[expireTime]" 其中****表示tokens的字符串编码形式
         * @param tokens
         * @return
         */
        public VersionedValue left(Collection<Token> tokens, long expireTime)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_LEFT,
                    makeTokenString(tokens),
                    Long.toString(expireTime)));
        }
        /**
         * 返回一个versionValue对象， value="moving,*" 其中*表示被移动的token的字符串编码形式
         * @param token
         * @return
         */
        public VersionedValue moving(Token token)
        {
            return new VersionedValue(VersionedValue.STATUS_MOVING + VersionedValue.DELIMITER + partitioner.getTokenFactory().toString(token));
        }
        /**
         * 返回一个versionValue对象， value="relocating,******" 其中******表示被移动的tokens的字符串编码形式
         * @param token
         * @return
         */
        public VersionedValue relocating(Collection<Token> srcTokens)
        {
            return new VersionedValue(
                    versionString(VersionedValue.STATUS_RELOCATING, StringUtils.join(srcTokens, VersionedValue.DELIMITER)));
        }

        public VersionedValue hostId(UUID hostId)
        {
            return new VersionedValue(hostId.toString());
        }
        /**
         * 将tokens编码成二进制形式，然后用iso8859-1编码成字符串生成VersionValue
         * @param tokens
         * @return
         */
        public VersionedValue tokens(Collection<Token> tokens)
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            try
            {
                TokenSerializer.serialize(partitioner, tokens, dos);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            return new VersionedValue(new String(bos.toByteArray(), ISO_8859_1));
        }
        /**
         * 返回一个versionValue对象， value="removing,*" 其中*表示host对应的uuid（不是token了..）
         * @param hostId
         * @return
         */
        public VersionedValue removingNonlocal(UUID hostId)
        {
            return new VersionedValue(versionString(VersionedValue.REMOVING_TOKEN, hostId.toString()));
        }
        /**
         * 返回一个versionValue对象， value="removed,*,*,[expireTime]" 其中*表示host对应的uuid（不是token了..）
         * @param hostId
         * @param expireTime
         * @return
         */
        public VersionedValue removedNonlocal(UUID hostId, long expireTime)
        {
            return new VersionedValue(versionString(VersionedValue.REMOVED_TOKEN, hostId.toString(), Long.toString(expireTime)));
        }
        /**
         * 返回一个versionValue对象， value="remover,*" 其中*表示host对应的uuid（不是token了..）
         * @param hostId
         * @return
         */
        public VersionedValue removalCoordinator(UUID hostId)
        {
            return new VersionedValue(versionString(VersionedValue.REMOVAL_COORDINATOR, hostId.toString()));
        }
        /**
         * 返回一个versionValue对象， value="hibernate,[true/false]"
         * @param value
         * @return
         */
        public VersionedValue hibernate(boolean value)
        {
            return new VersionedValue(VersionedValue.HIBERNATE + VersionedValue.DELIMITER + value);
        }
        /**
         * 返回一个versionValue对象， value="[datacenter]"
         * @param dcId
         * @return
         */
        public VersionedValue datacenter(String dcId)
        {
            return new VersionedValue(dcId);
        }
        /**
         * 返回一个versionValue对象， value="[rack]"
         * @param rackId
         * @return
         */
        public VersionedValue rack(String rackId)
        {
            return new VersionedValue(rackId);
        }
       /**
        * 返回一个versionValue对象， value="[endpoint.ip]" 
        * @param endpoint
        * @return
        */
        public VersionedValue rpcaddress(InetAddress endpoint)
        {
            return new VersionedValue(endpoint.getHostAddress());
        }
        /**
         * 返回一个versionValue对象， value="[本机的cassandra版本号]"
         * @return
         */
        public VersionedValue releaseVersion()
        {
            return new VersionedValue(FBUtilities.getReleaseVersionString());
        }
        /**
         * 返回一个versionValue对象， value="[MS记录的当前版本号]"
         * @return
         */
        public VersionedValue networkVersion()
        {
            return new VersionedValue(String.valueOf(MessagingService.current_version));
        }
        /**
         * 返回一个versionValue对象， value="[ip]"
         * @param private_ip
         * @return
         */
        public VersionedValue internalIP(String private_ip)
        {
            return new VersionedValue(private_ip);
        }
        /**
         * 返回一个versionValue对象， value="[value]"
         * @param value
         * @return
         */
        public VersionedValue severity(double value)
        {
            return new VersionedValue(String.valueOf(value));
        }
    }

    private static class VersionedValueSerializer implements IVersionedSerializer<VersionedValue>
    {
        public void serialize(VersionedValue value, DataOutput dos, int version) throws IOException
        {
            dos.writeUTF(outValue(value, version));
            dos.writeInt(value.version);
        }

        private String outValue(VersionedValue value, int version)
        {
            String outValue = value.value;

            if (version < MessagingService.VERSION_12)
            {
                String[] pieces = value.value.split(DELIMITER_STR, -1);
                String type = pieces[0];

                if ((type.equals(STATUS_NORMAL)) || type.equals(STATUS_BOOTSTRAPPING))
                {
                    assert pieces.length >= 2;
                    outValue = versionString(pieces[0], pieces[1]);
                }

                if (type.equals(STATUS_LEFT))
                {
                    assert pieces.length >= 3;

                    // three component 'left' was adopted starting from Cassandra 1.0
                    // previous versions have '<type>:<token>' format
                    outValue = (version < MessagingService.VERSION_10)
                                ? versionString(pieces[0], pieces[2])
                                : versionString(pieces[0], pieces[2], pieces[1]);
                }

                if ((type.equals(REMOVAL_COORDINATOR)) || (type.equals(REMOVING_TOKEN)) || (type.equals(REMOVED_TOKEN)))
                    throw new RuntimeException(String.format("Unable to serialize %s(%s...) for nodes older than 1.2",
                                                             VersionedValue.class.getName(), type));
            }
            return outValue;
        }

        public VersionedValue deserialize(DataInput dis, int version) throws IOException
        {
            String value = dis.readUTF();
            int valVersion = dis.readInt();
            return new VersionedValue(value, valVersion);
        }

        public long serializedSize(VersionedValue value, int version)
        {
            return TypeSizes.NATIVE.sizeof(outValue(value, version)) + TypeSizes.NATIVE.sizeof(value.version);
        }
    }
}

