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

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.*;

import org.apache.cassandra.utils.BiMultiValMap;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SortedBiMultiValMap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.service.StorageService;
/**
 * 一个token对应一个ip。一个ip可以对应多个token。
 * <br> token莫非表示的是一个虚拟节点？
 * @author hxd
 *
 */
public class TokenMetadata
{
    private static final Logger logger = LoggerFactory.getLogger(TokenMetadata.class);

    /**按照目前的写法，这里其实总是一个SortBiMultiValMap对象。 Maintains token to endpoint map of every node in the cluster. */
    private final BiMultiValMap<Token, InetAddress> tokenToEndpointMap;

    /** Maintains endpoint to host ID map of every node in the cluster */
    private final BiMap<InetAddress, UUID> endpointToHostIdMap;

    // Prior to CASSANDRA-603, we just had <tt>Map<Range, InetAddress> pendingRanges<tt>,
    // which was added to when a node began bootstrap and removed from when it finished.
    //节点开始启动的时候，加入pendingRanges，启动完毕删除之。
    // This is inadequate when multiple changes are allowed simultaneously.  For example,
    // suppose that there is a ring of nodes A, C and E, with replication factor 3.
    // Node D bootstraps between C and E, so its pending ranges will be E-A, A-C and C-D.
    // Now suppose node B bootstraps between A and C at the same time. Its pending ranges
    // would be C-E, E-A and A-B. Now both nodes need to be assigned pending range E-A,
    // which we would be unable to represent with the old Map.  The same thing happens
    // even more obviously for any nodes that boot simultaneously between same two nodes.
    //
    // So, we made two changes:
    //
    // First, we changed pendingRanges to a <tt>Multimap<Range, InetAddress></tt> (now
    // <tt>Map<String, Multimap<Range, InetAddress>></tt>, because replication strategy
    // and options are per-KeySpace).
    //
    // Second, we added the bootstrapTokens and leavingEndpoints collections, so we can
    // rebuild pendingRanges from the complete information of what is going on, when
    // additional changes are made mid-operation.
    //
    // Finally, note that recording the tokens of joining nodes in bootstrapTokens also
    // means we can detect and reject the addition of multiple nodes at the same token
    // before one becomes part of the ring.
    private final BiMultiValMap<Token, InetAddress> bootstrapTokens = new BiMultiValMap<Token, InetAddress>();
    // (don't need to record Token here since it's still part of tokenToEndpointMap until it's done leaving)
    private final Set<InetAddress> leavingEndpoints = new HashSet<InetAddress>();
    // this is a cache of the calculation from {tokenToEndpointMap, bootstrapTokens, leavingEndpoints}
    private final ConcurrentMap<String, Multimap<Range<Token>, InetAddress>> pendingRanges = new ConcurrentHashMap<String, Multimap<Range<Token>, InetAddress>>();
    /**
     * tokens which are migrating to new endpoints .为何要用一个set呢。。。 
     */
    // nodes which are migrating to the new tokens in the ring
    private final Set<Pair<Token, InetAddress>> movingEndpoints = new HashSet<Pair<Token, InetAddress>>();

    // tokens which are migrating to new endpoints
    private final ConcurrentMap<Token, InetAddress> relocatingTokens = new ConcurrentHashMap<Token, InetAddress>();

    /** Use this lock for manipulating the token map */
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private volatile ArrayList<Token> sortedTokens;

    private final Topology topology;
    /** list of subscribers that are notified when the tokenToEndpointMap changed */
    private final CopyOnWriteArrayList<AbstractReplicationStrategy> subscribers = new CopyOnWriteArrayList<AbstractReplicationStrategy>();
    /**
     * 网络ip比较器
     */
    private static final Comparator<InetAddress> inetaddressCmp = new Comparator<InetAddress>()
    {
        public int compare(InetAddress o1, InetAddress o2)
        {
            return ByteBuffer.wrap(o1.getAddress()).compareTo(ByteBuffer.wrap(o2.getAddress()));
        }
    };

    public TokenMetadata()
    {
        this(SortedBiMultiValMap.<Token, InetAddress>create(null, inetaddressCmp),
             HashBiMap.<InetAddress, UUID>create(),
             new Topology());
    }

    private TokenMetadata(BiMultiValMap<Token, InetAddress> tokenToEndpointMap, BiMap<InetAddress, UUID> endpointsMap, Topology topology)
    {
        this.tokenToEndpointMap = tokenToEndpointMap;
        this.topology = topology;
        endpointToHostIdMap = endpointsMap;
        sortedTokens = sortTokens();
    }
    /**
     * 返回tokenToEndpointMap的keySet（tokenToEndpointMap往往本身是有序的）
     * @return
     */
    private ArrayList<Token> sortTokens()
    {
        return new ArrayList<Token>(tokenToEndpointMap.keySet());
    }

    /** @return the number of nodes bootstrapping into source's primary range */
    public int pendingRangeChanges(InetAddress source)
    {
        int n = 0;
        Collection<Range<Token>> sourceRanges = getPrimaryRangesFor(getTokens(source));
        lock.readLock().lock();
        try
        {
            for (Token token : bootstrapTokens.keySet())
                for (Range<Token> range : sourceRanges)
                    if (range.contains(token))
                        n++;
        }
        finally
        {
            lock.readLock().unlock();
        }
        return n;
    }

    /**
     *  更新正常的token。该方法首先将该ip涉及的元素从各个容器中删除（会通知subscriber们清空token-endpoint缓存），然后再重新将token与ip的对应关系放入tokenToEndpointMap中<br>然后重新排序一下token
     * Update token map with a single token/endpoint pair in normal state.
     */
    public void updateNormalToken(Token token, InetAddress endpoint)
    {
        updateNormalTokens(Collections.singleton(token), endpoint);
    }
/**
 *  更新正常的token。该方法首先将该ip涉及的元素从各个容器中删除（会通知subscriber们清空token-endpoint缓存），然后再重新将token与ip的对应关系放入tokenToEndpointMap中<br>然后重新排序一下token
 * @param tokens
 * @param endpoint
 */
    public void updateNormalTokens(Collection<Token> tokens, InetAddress endpoint)
    {
        Multimap<InetAddress, Token> endpointTokens = HashMultimap.create();
        for (Token token : tokens)
            endpointTokens.put(endpoint, token);
        updateNormalTokens(endpointTokens);
    }

    /**
     * 更新正常的token。该方法首先将所有涉及的ip从各个容器中删除（会通知subscriber们清空token-endpoint缓存），然后再重新将token与这些ip的对应关系放入tokenToEndpointMap中
     * <br>然后重新排序一下token
     * Update token map with a set of token/endpoint pairs in normal state.
     *
     * Prefer this whenever there are multiple pairs to update, as each update (whether a single or multiple)
     * is expensive (CASSANDRA-3831).
     *
     * @param endpointTokens
     */
    public void updateNormalTokens(Multimap<InetAddress, Token> endpointTokens)
    {
        if (endpointTokens.isEmpty())
            return;

        lock.writeLock().lock();
        try
        {
            boolean shouldSortTokens = false;
            for (InetAddress endpoint : endpointTokens.keySet())
            {
                Collection<Token> tokens = endpointTokens.get(endpoint);

                assert tokens != null && !tokens.isEmpty();

                bootstrapTokens.removeValue(endpoint);//bootstrapToken中删除该ip的元素
                tokenToEndpointMap.removeValue(endpoint);//tokenToEndpointMap中删除该ip的元素
                topology.addEndpoint(endpoint);
                leavingEndpoints.remove(endpoint);
                removeFromMoving(endpoint); // also removing this endpoint from moving

                for (Token token : tokens)
                {
                    InetAddress prev = tokenToEndpointMap.put(token, endpoint);
                    if (!endpoint.equals(prev))
                    {
                        if (prev != null)
                            logger.warn("Token " + token + " changing ownership from " + prev + " to " + endpoint);
                        shouldSortTokens = true;
                    }
                }
            }

            if (shouldSortTokens)
                sortedTokens = sortTokens();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * 将ip-id存入endpointToHostIdMap中。如果这个id已经跟某个ip对应起来了，那么报错。 如果这个ip已经有一个id了，那么会打出一个警告，但是还是会修改...（与英文注释不一样啊。。。）
     * Store an end-point to host ID mapping.  Each ID must be unique, and
     * cannot be changed after the fact.
     *
     * @param hostId
     * @param endpoint
     */
    public void updateHostId(UUID hostId, InetAddress endpoint)
    {
        assert hostId != null;
        assert endpoint != null;

        InetAddress storedEp = endpointToHostIdMap.inverse().get(hostId);
        if (storedEp != null)
        {
            if (!storedEp.equals(endpoint) && (FailureDetector.instance.isAlive(storedEp)))//发现hostId对应的旧的ip与新提供的不等，并且旧的没有死，那么报错。
            {
                throw new RuntimeException(String.format("Host ID collision between active endpoint %s and %s (id=%s)",
                                                         storedEp,
                                                         endpoint,
                                                         hostId));
            }
        }

        UUID storedId = endpointToHostIdMap.get(endpoint);//判断下提供的ip是否已经有对应的id了。有的话 打出一个警告
        if ((storedId != null) && (!storedId.equals(hostId)))
            logger.warn("Changing {}'s host ID from {} to {}", new Object[] {endpoint, storedId, hostId});

        endpointToHostIdMap.forcePut(endpoint, hostId);
    }

    /** Return the unique host ID for an end-point. */
    public UUID getHostId(InetAddress endpoint)
    {
        return endpointToHostIdMap.get(endpoint);
    }

    /** Return the end-point for a unique host ID */
    public InetAddress getEndpointForHostId(UUID hostId)
    {
        return endpointToHostIdMap.inverse().get(hostId);
    }

    /** @return a copy of the endpoint-to-id map for read-only operations */
    public Map<InetAddress, UUID> getEndpointToHostIdMapForReading()
    {
        Map<InetAddress, UUID> readMap = new HashMap<InetAddress, UUID>();
        readMap.putAll(endpointToHostIdMap);
        return readMap;
    }

    @Deprecated
    public void addBootstrapToken(Token token, InetAddress endpoint)
    {
        addBootstrapTokens(Collections.singleton(token), endpoint);
    }
/**
 * 首先判断每个token是否已经出现在bootstrapTokens或tokenToEndpointMap中了，且对应的ip！=endpoint，那么报错
 * <br>然后从bootstrapTokens中先把这个endpoint对应的旧的元素删掉，然后将这个给强制加进去
 * @param tokens
 * @param endpoint
 */
    public void addBootstrapTokens(Collection<Token> tokens, InetAddress endpoint)
    {
        assert tokens != null && !tokens.isEmpty();
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {

            InetAddress oldEndpoint;

            for (Token token : tokens)
            {
                oldEndpoint = bootstrapTokens.get(token);
                if (oldEndpoint != null && !oldEndpoint.equals(endpoint))
                    throw new RuntimeException("Bootstrap Token collision between " + oldEndpoint + " and " + endpoint + " (token " + token);

                oldEndpoint = tokenToEndpointMap.get(token);
                if (oldEndpoint != null && !oldEndpoint.equals(endpoint))
                    throw new RuntimeException("Bootstrap Token collision between " + oldEndpoint + " and " + endpoint + " (token " + token);
            }

            bootstrapTokens.removeValue(endpoint);

            for (Token token : tokens)
                bootstrapTokens.put(token, endpoint);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
/**
 * 把所有的token相关的元素从bootstrapTokens中删除
 * @param tokens
 */
    public void removeBootstrapTokens(Collection<Token> tokens)
    {
        assert tokens != null && !tokens.isEmpty();

        lock.writeLock().lock();
        try
        {
            for (Token token : tokens)
                bootstrapTokens.remove(token);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
/**
 * 增加一个ip到leavingEndpoints
 * @param endpoint
 */
    public void addLeavingEndpoint(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            leavingEndpoints.add(endpoint);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * 加入一个token-ip对到movingEndpoints中
     * Add a new moving endpoint
     * @param token token which is node moving to
     * @param endpoint address of the moving node
     */
    public void addMovingEndpoint(Token token, InetAddress endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();

        try
        {
            movingEndpoints.add(Pair.create(token, endpoint));
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * 将token-ip放入relocatingToken中取
     * Add new relocating ranges (tokens moving from their respective endpoints, to another).
     * @param tokens tokens being moved
     * @param endpoint destination of moves
     */
    public void addRelocatingTokens(Collection<Token> tokens, InetAddress endpoint)
    {
        assert endpoint != null;
        assert tokens != null && tokens.size() > 0;

        lock.writeLock().lock();

        try
        {
            for (Token token : tokens)
            {
                InetAddress prev = relocatingTokens.put(token, endpoint);
                if (prev != null && !prev.equals(endpoint))
                    logger.warn("Relocation of {} to {} overwrites previous to {}", new Object[]{token, endpoint, prev});
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
/**
 * 从各个容器中删除与该ip相关的元素
 * @param endpoint
 */
    public void removeEndpoint(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            bootstrapTokens.removeValue(endpoint);
            tokenToEndpointMap.removeValue(endpoint);
            topology.removeEndpoint(endpoint);
            leavingEndpoints.remove(endpoint);
            endpointToHostIdMap.remove(endpoint);
            sortedTokens = sortTokens();
            invalidateCaches();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * 从movingEndpoints中删除pair的右边=该ip的元素对
     * <br>然后通知subcribers清空token-endpoint的缓存
     * Remove pair of token/address from moving endpoints
     * @param endpoint address of the moving node
     */
    public void removeFromMoving(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            for (Pair<Token, InetAddress> pair : movingEndpoints)
            {
                if (pair.right.equals(endpoint))
                {
                    movingEndpoints.remove(pair);
                    break;
                }
            }

            invalidateCaches();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * 根据token将之从
     * Remove pair of token/address from relocating ranges.
     * @param endpoint
     */
    public void removeFromRelocating(Token token, InetAddress endpoint)
    {
        assert endpoint != null;
        assert token != null;

        lock.writeLock().lock();

        try
        {
            InetAddress previous = relocatingTokens.remove(token);

            if (previous == null)
            {
                logger.debug("Cannot remove {}, not found among the relocating (previously removed?)", token);
            }
            else if (!previous.equals(endpoint))
            {
                logger.warn(
                        "Removal of relocating token {} with mismatched endpoint ({} != {})",
                        new Object[]{token, endpoint, previous});
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
/**
 * 从tokenToEndpointMap中获取ip对应的token
 * @param endpoint
 * @return
 */
    public Collection<Token> getTokens(InetAddress endpoint)
    {
        assert endpoint != null;
        assert isMember(endpoint); // don't want to return nulls

        lock.readLock().lock();
        try
        {
            return new ArrayList<Token>(tokenToEndpointMap.inverse().get(endpoint));
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Deprecated
    public Token getToken(InetAddress endpoint)
    {
        return getTokens(endpoint).iterator().next();
    }
/**
 * 判断ip在tokenToEndpointMap中出现过没有
 * @param endpoint
 * @return
 */
    public boolean isMember(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();
        try
        {
            return tokenToEndpointMap.inverse().containsKey(endpoint);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
/**
 * 判断ip在leavingEndpoints中出现过没有
 * @param endpoint
 * @return
 */
    public boolean isLeaving(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();
        try
        {
            return leavingEndpoints.contains(endpoint);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
/**
 * 判断ip在movingEndpoints中出现过没有
 * @param endpoint
 * @return
 */
    public boolean isMoving(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();

        try
        {
            for (Pair<Token, InetAddress> pair : movingEndpoints)
            {
                if (pair.right.equals(endpoint))
                    return true;
            }

            return false;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
/**
 * 判断token在relocatingTokens中出现过没有
 * @param token
 * @return
 */
    public boolean isRelocating(Token token)
    {
        assert token != null;

        lock.readLock().lock();

        try
        {
            return relocatingTokens.containsKey(token);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * Create a copy of TokenMetadata with only tokenToEndpointMap. That is, pending ranges,
     * bootstrap tokens and leaving endpoints are not included in the copy.
     */
    public TokenMetadata cloneOnlyTokenMap()
    {
        lock.readLock().lock();
        try
        {
            return new TokenMetadata(SortedBiMultiValMap.<Token, InetAddress>create(tokenToEndpointMap, null, inetaddressCmp),
                                     HashBiMap.create(endpointToHostIdMap),
                                     new Topology(topology));
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * 拷贝完tokenToEndpointMap后，从新的对象中将在leavingEndpoints中出现过的ip删除。
     * Create a copy of TokenMetadata with tokenToEndpointMap reflecting situation after all
     * current leave operations have finished.
     *
     * @return new token metadata
     */
    public TokenMetadata cloneAfterAllLeft()
    {
        lock.readLock().lock();
        try
        {
            TokenMetadata allLeftMetadata = cloneOnlyTokenMap();

            for (InetAddress endpoint : leavingEndpoints)
                allLeftMetadata.removeEndpoint(endpoint);

            return allLeftMetadata;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * 首先拷贝一份tokenToEndpointMap。然后删除所有在leavingEndpoints中的ip。然后将moving、relocating的ip都更新到正常状态下（放入tokenToEndpointMap中）
     * Create a copy of TokenMetadata with tokenToEndpointMap reflecting situation after all
     * current leave, move, and relocate operations have finished.
     *
     * @return new token metadata
     */
    public TokenMetadata cloneAfterAllSettled()
    {
        lock.readLock().lock();

        try
        {
            TokenMetadata metadata = cloneOnlyTokenMap();

            for (InetAddress endpoint : leavingEndpoints)
                metadata.removeEndpoint(endpoint);


            for (Pair<Token, InetAddress> pair : movingEndpoints)
                metadata.updateNormalToken(pair.left, pair.right);

            for (Map.Entry<Token, InetAddress> relocating: relocatingTokens.entrySet())
                metadata.updateNormalToken(relocating.getKey(), relocating.getValue());

            return metadata;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
/**
 * 从tokenToEndpointMap中获取token对应的ip
 * @param token
 * @return
 */
    public InetAddress getEndpoint(Token token)
    {
        lock.readLock().lock();
        try
        {
            return tokenToEndpointMap.get(token);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
/**
 * 根据每个token得到一个<他的前者，他>的range范围
 * @param tokens
 * @return
 */
    public Collection<Range<Token>> getPrimaryRangesFor(Collection<Token> tokens)
    {
        Collection<Range<Token>> ranges = new ArrayList<Range<Token>>(tokens.size());
        for (Token right : tokens)
            ranges.add(new Range<Token>(getPredecessor(right), right));
        return ranges;
    }
    /**
     * 根据token得到一个<他的前者，他>的range范围
     * @param tokens
     * @return
     */
    @Deprecated
    public Range<Token> getPrimaryRangeFor(Token right)
    {
        return getPrimaryRangesFor(Arrays.asList(right)).iterator().next();
    }

    public ArrayList<Token> sortedTokens()
    {
        return sortedTokens;
    }
/**
 * 从pendingRanges中得到对应的map。如果为空，则新建一个空的并放入pendingRanges中。
 * @param table
 * @return
 */
    private Multimap<Range<Token>, InetAddress> getPendingRangesMM(String table)
    {
        Multimap<Range<Token>, InetAddress> map = pendingRanges.get(table);
        if (map == null)
        {
            map = HashMultimap.create();
            Multimap<Range<Token>, InetAddress> priorMap = pendingRanges.putIfAbsent(table, map);
            if (priorMap != null)
                map = priorMap;
        }
        return map;
    }

    /**从pendingRanges中得到对应的map。如果为空，则新建一个空的并放入pendingRanges中。 但是返回值是一个标准的map。
     *  a mutable map may be returned but caller should not modify it */
    public Map<Range<Token>, Collection<InetAddress>> getPendingRanges(String table)
    {
        return getPendingRangesMM(table).asMap();
    }
/**
 * 从pendingRanges中得到对应的map。如果为空，则新建一个空的并放入pendingRanges中。 
 * <br>然后从这个map中（key为Range<token>，value为ip），获取ip等于endpoint的那些Range<token>。
 * @param table
 * @param endpoint
 * @return
 */
    public List<Range<Token>> getPendingRanges(String table, InetAddress endpoint)
    {
        List<Range<Token>> ranges = new ArrayList<Range<Token>>();
        for (Map.Entry<Range<Token>, InetAddress> entry : getPendingRangesMM(table).entries())
        {
            if (entry.getValue().equals(endpoint))
            {
                ranges.add(entry.getKey());
            }
        }
        return ranges;
    }
/**
 * 放入pendingRange中去
 * @param table
 * @param rangeMap
 */
    public void setPendingRanges(String table, Multimap<Range<Token>, InetAddress> rangeMap)
    {
        pendingRanges.put(table, rangeMap);
    }
/**
 * 二分查找法取离该token最近的前一个token。如果token排在第0位，则取最后一个
 * @param token
 * @return
 */
    public Token getPredecessor(Token token)
    {
        List tokens = sortedTokens();
        int index = Collections.binarySearch(tokens, token);
        assert index >= 0 : token + " not found in " + StringUtils.join(tokenToEndpointMap.keySet(), ", ");
        return (Token) (index == 0 ? tokens.get(tokens.size() - 1) : tokens.get(index - 1));
    }
/**
 * 二分查找法去离该token最近的后一个token。如果token排在最末尾，则取第一个。
 * @param token
 * @return
 */
    public Token getSuccessor(Token token)
    {
        List tokens = sortedTokens();
        int index = Collections.binarySearch(tokens, token);
        assert index >= 0 : token + " not found in " + StringUtils.join(tokenToEndpointMap.keySet(), ", ");
        return (Token) ((index == (tokens.size() - 1)) ? tokens.get(0) : tokens.get(index + 1));
    }

    /** @return a copy of the bootstrapping tokens map */
    public BiMultiValMap<Token, InetAddress> getBootstrapTokens()
    {
        lock.readLock().lock();
        try
        {
            return new BiMultiValMap<Token, InetAddress>(bootstrapTokens);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
/**
 * 返回endpointToHostIdMap中出现的所有ip
 * @return
 */
    public Set<InetAddress> getAllEndpoints()
    {
        return endpointToHostIdMap.keySet();
    }

    /** caller should not modify leavingEndpoints */
    public Set<InetAddress> getLeavingEndpoints()
    {
        return leavingEndpoints;
    }

    /**
     * Endpoints which are migrating to the new tokens
     * @return set of addresses of moving endpoints
     */
    public Set<Pair<Token, InetAddress>> getMovingEndpoints()
    {
        return movingEndpoints;
    }

    /**
     * Ranges which are migrating to new endpoints.
     * @return set of token-address pairs of relocating ranges
     */
    public Map<Token, InetAddress> getRelocatingRanges()
    {
        return relocatingTokens;
    }
/**
 * 找到start所在的位置。如果start不存在，则返回离他最近的左边的一个。 如果所有的都比start小，那么要么返回-1（insertMin=true） ，要么返回0
 * <br>所谓的insertMin,其实是指的将来iterator遍历时，是否包含StorageService.getPartitioner().getMinimumToken()。
 * @param ring
 * @param start
 * @param insertMin
 * @return
 */
    public static int firstTokenIndex(final ArrayList ring, Token start, boolean insertMin)
    {
        assert ring.size() > 0;
        // insert the minimum token (at index == -1) if we were asked to include it and it isn't a member of the ring
        int i = Collections.binarySearch(ring, start);//如果搜索键包含在列表中，则返回搜索键的索引；否则返回 (-(插入点) - 1)。插入点 被定义为将键插入列表的那一点：即第一个大于此键的元素索引；如果列表中的所有元素都小于指定的键，则为 list.size()。注意，这保证了当且仅当此键被找到时，返回的值将 >= 0。 
        if (i < 0)
        {
            i = (i + 1) * (-1);//表示最近的比start小的那个token的位置
            if (i >= ring.size())//如果所有的都比start小
                i = insertMin ? -1 : 0;
        }
        return i;
    }
/**
 * 找到start所在的位置的token。如果start不存在，则返回离他最近的左边的一个。 如果所有的都比start小，那么返回0
 * 然后得到ring.get(位置)。
 * @param ring
 * @param start
 * @return
 */
    public static Token firstToken(final ArrayList<Token> ring, Token start)
    {
        return ring.get(firstTokenIndex(ring, start, false));
    }

    /**
     * 从start开始，得到一个能转一圈的指针。
     * iterator over the Tokens in the given ring, starting with the token for the node owning start
     * (which does not have to be a Token in the ring)
     * @param includeMin True if the minimum token should be returned in the ring even if it has no owner.
     */
    public static Iterator<Token> ringIterator(final ArrayList<Token> ring, Token start, boolean includeMin)
    {
        if (ring.isEmpty())
            return includeMin ? Iterators.singletonIterator(StorageService.getPartitioner().getMinimumToken())
                              : Iterators.<Token>emptyIterator();

        final boolean insertMin = includeMin && !ring.get(0).isMinimum();
        final int startIndex = firstTokenIndex(ring, start, insertMin);
        return new AbstractIterator<Token>()
        {
            int j = startIndex;
            protected Token computeNext()
            {
                if (j < -1)
                    return endOfData();
                try
                {
                    // return minimum for index == -1
                    if (j == -1)
                        return StorageService.getPartitioner().getMinimumToken();
                    // return ring token for other indexes
                    return ring.get(j);
                }
                finally
                {
                    j++;
                    if (j == ring.size())
                        j = insertMin ? -1 : 0;
                    if (j == startIndex)
                        // end iteration
                        j = -2;
                }
            }
        };
    }

    /** used by tests */
    public void clearUnsafe()
    {
        bootstrapTokens.clear();
        tokenToEndpointMap.clear();
        topology.clear();
        leavingEndpoints.clear();
        pendingRanges.clear();
        endpointToHostIdMap.clear();
        invalidateCaches();
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        lock.readLock().lock();
        try
        {
            Set<InetAddress> eps = tokenToEndpointMap.inverse().keySet();

            if (!eps.isEmpty())
            {
                sb.append("Normal Tokens:");
                sb.append(System.getProperty("line.separator"));
                for (InetAddress ep : eps)
                {
                    sb.append(ep);
                    sb.append(":");
                    sb.append(tokenToEndpointMap.inverse().get(ep));
                    sb.append(System.getProperty("line.separator"));
                }
            }

            if (!bootstrapTokens.isEmpty())
            {
                sb.append("Bootstrapping Tokens:" );
                sb.append(System.getProperty("line.separator"));
                for (Map.Entry<Token, InetAddress> entry : bootstrapTokens.entrySet())
                {
                    sb.append(entry.getValue()).append(":").append(entry.getKey());
                    sb.append(System.getProperty("line.separator"));
                }
            }

            if (!leavingEndpoints.isEmpty())
            {
                sb.append("Leaving Endpoints:");
                sb.append(System.getProperty("line.separator"));
                for (InetAddress ep : leavingEndpoints)
                {
                    sb.append(ep);
                    sb.append(System.getProperty("line.separator"));
                }
            }

            if (!pendingRanges.isEmpty())
            {
                sb.append("Pending Ranges:");
                sb.append(System.getProperty("line.separator"));
                sb.append(printPendingRanges());
            }
        }
        finally
        {
            lock.readLock().unlock();
        }

        return sb.toString();
    }

    public String printPendingRanges()
    {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, Multimap<Range<Token>, InetAddress>> entry : pendingRanges.entrySet())
        {
            for (Map.Entry<Range<Token>, InetAddress> rmap : entry.getValue().entries())
            {
                sb.append(rmap.getValue()).append(":").append(rmap.getKey());
                sb.append(System.getProperty("line.separator"));
            }
        }

        return sb.toString();
    }

    public String printRelocatingRanges()
    {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<Token, InetAddress> entry : relocatingTokens.entrySet())
            sb.append(String.format("%s:%s%n", entry.getKey(), entry.getValue()));

        return sb.toString();
    }
/**
 * 通知subscribers，清空token-endpoint的缓存。
 */
    public void invalidateCaches()
    {
        for (AbstractReplicationStrategy subscriber : subscribers)
        {
            subscriber.invalidateCachedTokenEndpointValues();
        }
    }
/**
 * 添加到subscribers中，目前发现有strategy进行了注册
 * @param subscriber
 */
    public void register(AbstractReplicationStrategy subscriber)
    {
        subscribers.add(subscriber);
    }
/**
 * 从subscriber中移除
 * @param subscriber
 */
    public void unregister(AbstractReplicationStrategy subscriber)
    {
        subscribers.remove(subscriber);
    }
/**
 * 根据table从pendingRanges中找到素有相关的range-ips的map。然后判断哪个range包含指定的token，就返回这些ip。
 * @param token
 * @param table
 * @return
 */
    public Collection<InetAddress> pendingEndpointsFor(Token token, String table)
    {
        Map<Range<Token>, Collection<InetAddress>> ranges = getPendingRanges(table);
        if (ranges.isEmpty())
            return Collections.emptyList();

        Set<InetAddress> endpoints = new HashSet<InetAddress>();
        for (Map.Entry<Range<Token>, Collection<InetAddress>> entry : ranges.entrySet())
        {
            if (entry.getKey().contains(token))
                endpoints.addAll(entry.getValue());
        }

        return endpoints;
    }

    /** 
     * 得到pending中所有的和token相关的ip以及naturalEndpoints。
     * @deprecated retained for benefit of old tests
     */
    public Collection<InetAddress> getWriteEndpoints(Token token, String table, Collection<InetAddress> naturalEndpoints)
    {
        ArrayList<InetAddress> endpoints = new ArrayList<InetAddress>();
        Iterables.addAll(endpoints, Iterables.concat(naturalEndpoints, pendingEndpointsFor(token, table)));
        return endpoints;
    }

    /** @return an endpoint to token multimap representation of tokenToEndpointMap (a copy) */
    public Multimap<InetAddress, Token> getEndpointToTokenMapForReading()
    {
        lock.readLock().lock();
        try
        {
            Multimap<InetAddress, Token> cloned = HashMultimap.create();
            for (Map.Entry<Token, InetAddress> entry : tokenToEndpointMap.entrySet())
                cloned.put(entry.getValue(), entry.getKey());
            return cloned;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * @return a (stable copy, won't be modified) Token to Endpoint map for all the normal and bootstrapping nodes
     *         in the cluster.
     */
    public Map<Token, InetAddress> getNormalAndBootstrappingTokenToEndpointMap()
    {
        lock.readLock().lock();
        try
        {
            Map<Token, InetAddress> map = new HashMap<Token, InetAddress>(tokenToEndpointMap.size() + bootstrapTokens.size());
            map.putAll(tokenToEndpointMap);
            map.putAll(bootstrapTokens);
            return map;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * @return the Topology map of nodes to DCs + Racks
     *
     * This is only allowed when a copy has been made of TokenMetadata, to avoid concurrent modifications
     * when Topology methods are subsequently used by the caller.
     */
    public Topology getTopology()
    {
        assert this != StorageService.instance.getTokenMetadata();
        return topology;
    }

    /**
     * Tracks the assignment of racks and endpoints in each datacenter for all the "normal" endpoints
     * in this TokenMetadata. This allows faster calculation of endpoints in NetworkTopologyStrategy.
     */
    public static class Topology
    {
        /** multi-map of DC to endpoints in that DC */
        private final Multimap<String, InetAddress> dcEndpoints;
        /** map of DC to multi-map of rack to endpoints in that rack */
        private final Map<String, Multimap<String, InetAddress>> dcRacks;
        /** reverse-lookup map for endpoint to current known dc/rack assignment */
        private final Map<InetAddress, Pair<String, String>> currentLocations;

        protected Topology()
        {
            dcEndpoints = HashMultimap.create();
            dcRacks = new HashMap<String, Multimap<String, InetAddress>>();
            currentLocations = new HashMap<InetAddress, Pair<String, String>>();
        }

        protected void clear()
        {
            dcEndpoints.clear();
            dcRacks.clear();
            currentLocations.clear();
        }

        /**
         * construct deep-copy of other
         */
        protected Topology(Topology other)
        {
            dcEndpoints = HashMultimap.create(other.dcEndpoints);
            dcRacks = new HashMap<String, Multimap<String, InetAddress>>();
            for (String dc : other.dcRacks.keySet())
                dcRacks.put(dc, HashMultimap.create(other.dcRacks.get(dc)));
            currentLocations = new HashMap<InetAddress, Pair<String, String>>(other.currentLocations);
        }

        /**
         * Stores current DC/rack assignment for ep
         */
        protected void addEndpoint(InetAddress ep)
        {
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            String dc = snitch.getDatacenter(ep);
            String rack = snitch.getRack(ep);
            Pair<String, String> current = currentLocations.get(ep);
            if (current != null)
            {
                if (current.left.equals(dc) && current.right.equals(rack))
                    return;
                dcRacks.get(current.left).remove(current.right, ep);
                dcEndpoints.remove(current.left, ep);
            }

            dcEndpoints.put(dc, ep);

            if (!dcRacks.containsKey(dc))
                dcRacks.put(dc, HashMultimap.<String, InetAddress>create());
            dcRacks.get(dc).put(rack, ep);

            currentLocations.put(ep, Pair.create(dc, rack));
        }

        /**
         * Removes current DC/rack assignment for ep
         */
        protected void removeEndpoint(InetAddress ep)
        {
            if (!currentLocations.containsKey(ep))
                return;
            Pair<String, String> current = currentLocations.remove(ep);
            dcEndpoints.remove(current.left, ep);
            dcRacks.get(current.left).remove(current.right, ep);
        }

        /**
         * @return multi-map of DC to endpoints in that DC
         */
        public Multimap<String, InetAddress> getDatacenterEndpoints()
        {
            return dcEndpoints;
        }

        /**
         * @return map of DC to multi-map of rack to endpoints in that rack
         */
        public Map<String, Multimap<String, InetAddress>> getDatacenterRacks()
        {
            return dcRacks;
        }
    }
}
