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
package org.apache.cassandra.dht;

/**
 * Interface representing a position on the ring.
 * Both Token and DecoratedKey represent a position in the ring, a token being
 * less precise than a DecoratedKey (a token is really a range of keys).
 * token没有decoratedKey精确？ 一个token是一个范围的keys.其实 Token集成的是RingPosition。而DecoratedKey继承的是RowPosition（RowPosition又集成了RingPosition）,
 * 而且DecoratedKey的组成其实是一个token和一个key，因此必然更准确。。
 *///
public interface RingPosition<T> extends Comparable<T>
{
    public Token getToken();
    public boolean isMinimum(IPartitioner partitioner);
}
