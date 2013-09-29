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
package org.apache.cassandra.locator;

import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.DatacenterSyncWriteResponseHandler;
import org.apache.cassandra.service.DatacenterWriteResponseHandler;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.utils.FBUtilities;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 *主要方法是 根据toekn获取在哪个机器上。
 * A abstract parent for all replication strategies.
*/
public abstract class AbstractReplicationStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractReplicationStrategy.class);

    @VisibleForTesting
    final String tableName;
    private Table table;
    public final Map<String, String> configOptions;
    private final TokenMetadata tokenMetadata;

    public IEndpointSnitch snitch;

    AbstractReplicationStrategy(String tableName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions)
    {
        assert tableName != null;
        assert snitch != null;
        assert tokenMetadata != null;
        this.tokenMetadata = tokenMetadata;
        this.snitch = snitch;
        this.tokenMetadata.register(this);
        this.configOptions = configOptions == null ? Collections.<String, String>emptyMap() : configOptions;
        this.tableName = tableName;
        // lazy-initialize table itself since we don't create them until after the replication strategies
    }

    private final Map<Token, ArrayList<InetAddress>> cachedEndpoints = new NonBlockingHashMap<Token, ArrayList<InetAddress>>();

    public ArrayList<InetAddress> getCachedEndpoints(Token t)
    {
        return cachedEndpoints.get(t);
    }
/**
 * 村放入cachedEndpoints中
 * @param t
 * @param addr
 */
    public void cacheEndpoint(Token t, ArrayList<InetAddress> addr)
    {
        cachedEndpoints.put(t, addr);
    }
/**
 * 清空cachedEndpoints
 */
    public void clearEndpointCache()
    {
        logger.debug("clearing cached endpoints");
        cachedEndpoints.clear();
    }

    /**
     * 得到那些应该存储着给定的toeken的（可能被缓存的）endpoints。虽然返回的endpoints应该是一个没用重复的set，但是为了避免额外的排序，我们用list返回
     * <br> 如果cachedEndpoints中没有的话，通过calculateNaturalEndpoints得到并保存。
     * get the (possibly cached) endpoints that should store the given Token.
     * Note that while the endpoints are conceptually a Set (no duplicates will be included),
     * we return a List to avoid an extra allocation when sorting by proximity later
     * @param searchPosition the position the natural endpoints are requested for
     * @return a copy of the natural endpoints for the given token
     */
    public ArrayList<InetAddress> getNaturalEndpoints(RingPosition searchPosition)
    {
        Token searchToken = searchPosition.getToken();
        Token keyToken = TokenMetadata.firstToken(tokenMetadata.sortedTokens(), searchToken);
        ArrayList<InetAddress> endpoints = getCachedEndpoints(keyToken);
        if (endpoints == null)
        {
            TokenMetadata tokenMetadataClone = tokenMetadata.cloneOnlyTokenMap();
            keyToken = TokenMetadata.firstToken(tokenMetadataClone.sortedTokens(), searchToken);
            endpoints = new ArrayList<InetAddress>(calculateNaturalEndpoints(searchToken, tokenMetadataClone));
            cacheEndpoint(keyToken, endpoints);
        }

        return new ArrayList<InetAddress>(endpoints);
    }

    /**
     * calculate the natural endpoints for the given token
     *
     * @see #getNaturalEndpoints(org.apache.cassandra.dht.RingPosition)
     *
     * @param searchToken the token the natural endpoints are requested for
     * @return a copy of the natural endpoints for the given token
     */
    public abstract List<InetAddress> calculateNaturalEndpoints(Token searchToken, TokenMetadata tokenMetadata);
/**
 * 基本上是直接调用了构造函数。
 * 如果一致性级别是Local_quorum则生成DatacenterWriteResponseHandler<br>
 * 如果一致性级别是each_quorum，则生成DatacenterSyncWriteResponseHandler<br>
 * 否则生成WriteResponseHandler
 * @param naturalEndpoints
 * @param pendingEndpoints
 * @param consistency_level
 * @param callback
 * @param writeType
 * @return
 */
    public AbstractWriteResponseHandler getWriteResponseHandler(Collection<InetAddress> naturalEndpoints, Collection<InetAddress> pendingEndpoints, ConsistencyLevel consistency_level, Runnable callback, WriteType writeType)
    {
        if (consistency_level == ConsistencyLevel.LOCAL_QUORUM)
        {
            // block for in this context will be localnodes block.
            return new DatacenterWriteResponseHandler(naturalEndpoints, pendingEndpoints, consistency_level, getTable(), callback, writeType);
        }
        else if (consistency_level == ConsistencyLevel.EACH_QUORUM)
        {
            return new DatacenterSyncWriteResponseHandler(naturalEndpoints, pendingEndpoints, consistency_level, getTable(), callback, writeType);
        }
        return new WriteResponseHandler(naturalEndpoints, pendingEndpoints, consistency_level, getTable(), callback, writeType);
    }
/**
 * Table.open 然后返回table
 * @return
 */
    private Table getTable()
    {
        if (table == null)
            table = Table.open(tableName);
        return table;
    }

    /**
     *abstract 基于strategy_options得到备份数。注意这个方法要保证非常快，因为经常被调用
     * calculate the RF based on strategy_options. When overwriting, ensure that this get()
     *  is FAST, as this is called often.
     *
     * @return the replication factor
     */
    public abstract int getReplicationFactor();

    /**
     * 对metadata中sortedToken中的每个token进行二分查找，生成range对，并计算这个token对应的ip是多少。然后保存ip-range对
     * <br>效率不高,for循环的调用二分查找...
     * NOTE: this is pretty inefficient. also the inverse (getRangeAddresses) below.
     * this is fine as long as we don't use this on any critical path.
     * (fixing this would probably require merging tokenmetadata into replicationstrategy,
     * so we could cache/invalidate cleanly.)
     */
    public Multimap<InetAddress, Range<Token>> getAddressRanges(TokenMetadata metadata)
    {
        Multimap<InetAddress, Range<Token>> map = HashMultimap.create();

        for (Token token : metadata.sortedTokens())
        {
            @SuppressWarnings("deprecation")
			Range<Token> range = metadata.getPrimaryRangeFor(token);//二分查找
            for (InetAddress ep : calculateNaturalEndpoints(token, metadata))
            {
                map.put(ep, range);
            }
        }

        return map;
    }
/**
 * 对metadata中sortedToken中的每个token进行二分查找，生成range对，并计算这个token对应的ip是多少。然后保存range-ip对
 * <br>效率不高,for循环的调用二分查找...
 * @param metadata
 * @return
 */
    public Multimap<Range<Token>, InetAddress> getRangeAddresses(TokenMetadata metadata)
    {
        Multimap<Range<Token>, InetAddress> map = HashMultimap.create();

        for (Token token : metadata.sortedTokens())
        {
            Range<Token> range = metadata.getPrimaryRangeFor(token);
            for (InetAddress ep : calculateNaturalEndpoints(token, metadata))
            {
                map.put(range, ep);
            }
        }

        return map;
    }
/**
 * 对metadata中sortedToken中的每个token进行二分查找，生成range对，并计算这个token对应的ip是多少。然后保存ip-range对
 * <br>效率不高,for循环的调用二分查找...
 * @return
 */
    public Multimap<InetAddress, Range<Token>> getAddressRanges()
    {
        return getAddressRanges(tokenMetadata.cloneOnlyTokenMap());
    }

    public Collection<Range<Token>> getPendingAddressRanges(TokenMetadata metadata, Token pendingToken, InetAddress pendingAddress)
    {
        return getPendingAddressRanges(metadata, Arrays.asList(pendingToken), pendingAddress);
    }
/**
 * 将pendingTokens中的元素更新到tokenToEndpoints中，然后二分查找metadata中的所有token，生成range，生成ip-range对... 然后获取有关的ip对应的range。。
 * @param metadata
 * @param pendingTokens
 * @param pendingAddress
 * @return
 */
    public Collection<Range<Token>> getPendingAddressRanges(TokenMetadata metadata, Collection<Token> pendingTokens, InetAddress pendingAddress)
    {
        TokenMetadata temp = metadata.cloneOnlyTokenMap();
        temp.updateNormalTokens(pendingTokens, pendingAddress);
        return getAddressRanges(temp).get(pendingAddress);
    }
	/**
	 * 清空endpointCache
	 */
    public void invalidateCachedTokenEndpointValues()
    {
        clearEndpointCache();
    }

    public abstract void validateOptions() throws ConfigurationException;

    /**
     * 定义那些参数应该被识别，如果返回empty 则表示什么都不接受，返回null表示什么都接受
     * The options recognized by the strategy.
     * The empty collection means that no options are accepted, but null means
     * that any option is accepted.
     */
    public Collection<String> recognizedOptions()
    {
        // We default to null for backward compatibility sake
        return null;
    }
/**
 * 创建一个replicationStrategy实例
 * @param table
 * @param strategyClass
 * @param tokenMetadata
 * @param snitch
 * @param strategyOptions
 * @return
 * @throws ConfigurationException
 */
    private static AbstractReplicationStrategy createInternal(String table,
                                                              Class<? extends AbstractReplicationStrategy> strategyClass,
                                                              TokenMetadata tokenMetadata,
                                                              IEndpointSnitch snitch,
                                                              Map<String, String> strategyOptions)
        throws ConfigurationException
    {
        AbstractReplicationStrategy strategy;
        Class [] parameterTypes = new Class[] {String.class, TokenMetadata.class, IEndpointSnitch.class, Map.class};
        try
        {
            Constructor<? extends AbstractReplicationStrategy> constructor = strategyClass.getConstructor(parameterTypes);
            strategy = constructor.newInstance(table, tokenMetadata, snitch, strategyOptions);
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Error constructing replication strategy class", e);
        }
        return strategy;
    }
/**
 * 创建一个replicationStrategy实例。然后验证其参数。如果有无法识别的参数，log4j输出警告，然后忽略之。
 * @param table
 * @param strategyClass
 * @param tokenMetadata
 * @param snitch
 * @param strategyOptions
 * @return
 */
    public static AbstractReplicationStrategy createReplicationStrategy(String table,
                                                                        Class<? extends AbstractReplicationStrategy> strategyClass,
                                                                        TokenMetadata tokenMetadata,
                                                                        IEndpointSnitch snitch,
                                                                        Map<String, String> strategyOptions)
    {
        try
        {
            AbstractReplicationStrategy strategy = createInternal(table, strategyClass, tokenMetadata, snitch, strategyOptions);

            // Because we used to not properly validate unrecognized options, we only log a warning if we find one.
            try
            {
                strategy.validateExpectedOptions();
            }
            catch (ConfigurationException e)
            {
                logger.warn("Ignoring {}", e.getMessage());
            }

            strategy.validateOptions();
            return strategy;
        }
        catch (ConfigurationException e)
        {
            // If that happens at this point, there is nothing we can do about it.
            throw new RuntimeException();
        }
    }
/**
 * 创建一个实例，然后验证之，如果有不可识别的参数 抛出异常：ConfigurationException
 * @param table
 * @param strategyClassName
 * @param tokenMetadata
 * @param snitch
 * @param strategyOptions
 * @throws ConfigurationException
 */
    public static void validateReplicationStrategy(String table,
                                                   String strategyClassName,
                                                   TokenMetadata tokenMetadata,
                                                   IEndpointSnitch snitch,
                                                   Map<String, String> strategyOptions) throws ConfigurationException
    {
        AbstractReplicationStrategy strategy = createInternal(table, getClass(strategyClassName), tokenMetadata, snitch, strategyOptions);
        strategy.validateExpectedOptions();
        strategy.validateOptions();
    }
/**
 * 创建一个实例，然后验证之，忽略检查是否有不可识别的参数 
 * For backward compatibility sake on the thrift side
 * @param table
 * @param strategyClass
 * @param tokenMetadata
 * @param snitch
 * @param strategyOptions
 * @throws ConfigurationException
 */
    // For backward compatibility sake on the thrift side
    public static void validateReplicationStrategyIgnoreUnexpected(String table,
                                                                   Class<? extends AbstractReplicationStrategy> strategyClass,
                                                                   TokenMetadata tokenMetadata,
                                                                   IEndpointSnitch snitch,
                                                                   Map<String, String> strategyOptions) throws ConfigurationException
    {
        AbstractReplicationStrategy strategy = createInternal(table, strategyClass, tokenMetadata, snitch, strategyOptions);
        strategy.validateOptions();
    }
/**
 * 根据类名得到一个replicationStrategy的实例（空构造函数），如果该实例没有实现本接口，抛出异常
 * @param cls
 * @return
 * @throws ConfigurationException
 */
    public static Class<AbstractReplicationStrategy> getClass(String cls) throws ConfigurationException
    {
        String className = cls.contains(".") ? cls : "org.apache.cassandra.locator." + cls;
        Class<AbstractReplicationStrategy> strategyClass = FBUtilities.classForName(className, "replication strategy");
        if (!AbstractReplicationStrategy.class.isAssignableFrom(strategyClass))
        {
            throw new ConfigurationException(String.format("Specified replication strategy class (%s) is not derived from AbstractReplicationStrategy", className));
        }
        return strategyClass;
    }
/**
 * 判断下复制数是否是正整数
 * @param rf
 * @throws ConfigurationException
 */
    protected void validateReplicationFactor(String rf) throws ConfigurationException
    {
        try
        {
            if (Integer.parseInt(rf) < 0)
            {
                throw new ConfigurationException("Replication factor must be non-negative; found " + rf);
            }
        }
        catch (NumberFormatException e2)
        {
            throw new ConfigurationException("Replication factor must be numeric; found " + rf);
        }
    }
/**
 * 验证有没有不认识的参数。 认识的参数用recognizedOptions获得。
 * @throws ConfigurationException
 */
    private void validateExpectedOptions() throws ConfigurationException
    {
        Collection expectedOptions = recognizedOptions();
        if (expectedOptions == null)
            return;

        for (String key : configOptions.keySet())
        {
            if (!expectedOptions.contains(key))
                throw new ConfigurationException(String.format("Unrecognized strategy option {%s} passed to %s for keyspace %s", key, getClass().getSimpleName(), tableName));
        }
    }
}
