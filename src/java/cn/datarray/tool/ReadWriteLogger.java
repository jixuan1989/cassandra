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

package cn.datarray.tool;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ReadWriteLogger {
    private static Logger LOGGER = LoggerFactory.getLogger(ReadWriteLogger.class);

    public static void logStatement(String statement, String ip) {
        LOGGER.info("CQL from {} : {}", ip, statement);
    }
    public static void logPrepared(int id, String statement, String ip) {
        LOGGER.info("CQL Prepared from {} : {}, id: {}", ip, statement, id);
    }

    public static void logRunPrepared(int id, String ip) {
        LOGGER.info("Prepared CMD {} is executed by {}", id, ip);
    }
    public static void logBatchStatement(int count, String ip) {
        LOGGER.info("A batch statement with {} CMDs is executed by {}", count, ip);
    }


    public static void logGetSlice(ByteBuffer key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level, String ip) {
        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("get slice query, key: {}, parent: {}, predicate: {}, level: {}, ip: {}", getString(key), getString(column_parent), getString(predicate), consistency_level.name(), ip);
        }
    }

    public static void logMultiGetSlice(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level, String ip) {
        if(LOGGER.isDebugEnabled()) {
            StringBuilder stringBuilder =new StringBuilder(keys.size()*10);
            for(ByteBuffer key: keys) {
                stringBuilder.append(getString(key)).append(",");
            }
            LOGGER.debug("multi get slice query, key: {}, parent: {}, predicate: {}, level: {}, ip: {}", stringBuilder.toString(), getString(column_parent), getString(predicate), consistency_level.name(), ip);
        }
    }

    public static void logGet(ByteBuffer key, ColumnPath column_path, ConsistencyLevel consistency_level, String ip) {
        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("get query, key: {}, column_path: {}, level: {}, ip: {}", getString(key), getString(column_path),  consistency_level.name(), ip);
        }
    }

    public static void logGetCount(ByteBuffer key, ColumnParent column_parent, ConsistencyLevel consistency_level, String ip)
    {
        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("get count query, key: {}, parent: {},  level: {}, ip: {}", getString(key), getString(column_parent),  consistency_level.name(), ip);
        }
    }

    public static void logMultiGetCount(List<ByteBuffer> keys, ColumnParent column_parent, ConsistencyLevel consistency_level, String ip)
    {
        if(LOGGER.isDebugEnabled()) {
            StringBuilder stringBuilder =new StringBuilder(keys.size()*10);
            for(ByteBuffer key: keys) {
                stringBuilder.append(getString(key)).append(",");;
            }
            LOGGER.debug("multi get count query, key: {}, column_parent: {}, level: {}, ip: {}", stringBuilder.toString(), getString(column_parent),  consistency_level.name(), ip);
        }
    }

    public static void logGetRangeSlice(ColumnParent column_parent, SlicePredicate predicate, KeyRange range, ConsistencyLevel consistency_level, String ip)
    {
        if(LOGGER.isDebugEnabled()) {

            LOGGER.debug("multi get count query, column_parent: {}, predicate: {}, range: {}, level: {}, ip: {}", getString(column_parent), getString(predicate), range.toString(), consistency_level.name(), ip);
        }
    }

    public static void logThriftCql3Query(String statement, String ip)
    {
        LOGGER.info("CQL (Thrift) from {} : {}", ip, statement);
    }

    public static void logPrepareThriftCql3Query(String statement, String ip)
    {
        LOGGER.info("set prepare CQL (Thrift) from {} : {}", ip, statement);
    }
    private static String getString(ByteBuffer key) {
        try
        {
            return ByteBufferUtil.string(key);
        }
        catch (CharacterCodingException e)
        {
            return ByteBufferUtil.bytesToHex(key);
        }
    }

    private static String getString(ColumnParent parent) {
        return "(CF) " +parent.column_family;
    }
    private static String getString(ColumnPath column) {
        return "(CF) " + column.column_family +", (Column) " + getString(column.column);
    }
    private static String getString(SliceRange range) {
        return "(ST) " + getString(range.start) +", (ET) " +getString(range.finish);
    }
    private static String getString(SlicePredicate predicate) {
        StringBuilder builder = new StringBuilder();
        if(predicate.isSetColumn_names()) {
            builder.append("(Column Names) ");
            for(ByteBuffer key: predicate.column_names) {
                builder.append(getString(key) +", ");
            }
        }
        if (predicate.isSetSlice_range()) {
            builder.append("(Slice Range) ");
            builder.append(getString(predicate.slice_range));
        }
        return builder.toString();
    }
}
