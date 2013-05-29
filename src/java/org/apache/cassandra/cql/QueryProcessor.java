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
package org.apache.cassandra.cql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.*;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cli.CliUtils;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlMetadata;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlResultType;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.thrift.ThriftClientState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SemanticVersion;
import org.antlr.runtime.*;


import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;

public class QueryProcessor
{
    public static final SemanticVersion CQL_VERSION = new SemanticVersion("2.0.0");

    private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);

    public static final String DEFAULT_KEY_NAME = bufferToString(CFMetaData.DEFAULT_KEY_NAME);

    public static String oriQueryString = "";
    
    private static List<org.apache.cassandra.db.Row> getSlice(CFMetaData metadata, SelectStatement select, List<ByteBuffer> variables)
    throws InvalidRequestException, ReadTimeoutException, UnavailableException, IsBootstrappingException
    {
        QueryPath queryPath = new QueryPath(select.getColumnFamily());
        List<ReadCommand> commands = new ArrayList<ReadCommand>();

        // ...of a list of column names
        if (!select.isColumnRange())
        {
            Collection<ByteBuffer> columnNames = getColumnNames(select, metadata, variables);
            validateColumnNames(columnNames);

            for (Term rawKey: select.getKeys())
            {
                ByteBuffer key = rawKey.getByteBuffer(metadata.getKeyValidator(),variables);

                validateKey(key);
                commands.add(new SliceByNamesReadCommand(metadata.ksName, key, queryPath, columnNames));
            }
        }
        // ...a range (slice) of column names
        else
        {
            AbstractType<?> comparator = select.getComparator(metadata.ksName);
            ByteBuffer start = select.getColumnStart().getByteBuffer(comparator,variables);
            ByteBuffer finish = select.getColumnFinish().getByteBuffer(comparator,variables);

            for (Term rawKey : select.getKeys())
            {
                ByteBuffer key = rawKey.getByteBuffer(metadata.getKeyValidator(),variables);

                validateKey(key);
                validateSliceFilter(metadata, start, finish, select.isColumnsReversed());
                commands.add(new SliceFromReadCommand(metadata.ksName,
                                                      key,
                                                      queryPath,
                                                      start,
                                                      finish,
                                                      select.isColumnsReversed(),
                                                      select.getColumnsLimit()));
            }
        }

        return StorageProxy.read(commands, select.getConsistencyLevel());
    }

    private static List<org.apache.cassandra.db.Row> getSliceForSpecializedSelect(CFMetaData metadata, SelectStatement select, List<ByteBuffer> variables)
    	    throws InvalidRequestException, ReadTimeoutException, UnavailableException, IsBootstrappingException
    {
        QueryPath queryPath = new QueryPath(select.getColumnFamily());
        List<ReadCommand> commands = new ArrayList<ReadCommand>();

        // ...of a list of column names
        if (!select.isColumnRange())
        {
            Collection<ByteBuffer> columnNames = getColumnNames(select, metadata, variables);
            validateColumnNames(columnNames);

            for (Term rawKey: select.getKeys())
            {
                ByteBuffer key = rawKey.getByteBuffer(metadata.getKeyValidator(),variables);

                validateKey(key);
                commands.add(new SliceByNamesReadCommand(metadata.ksName, key, queryPath, columnNames));
            }
        }
        // ...a range (slice) of column names
        else
        {
            AbstractType<?> comparator = select.getComparator(metadata.ksName);
            ByteBuffer start = select.getColumnStart().getByteBuffer(comparator,variables);
            ByteBuffer finish = select.getColumnFinish().getByteBuffer(comparator,variables);

            for (Term rawKey : select.getKeys())
            {
                ByteBuffer key = rawKey.getByteBuffer(metadata.getKeyValidator(),variables);

                validateKey(key);
                validateSliceFilter(metadata, start, finish, select.isColumnsReversed());
                commands.add(new SliceFromReadCommand(metadata.ksName,
                                                      key,
                                                      queryPath,
                                                      start,
                                                      finish,
                                                      select.isColumnsReversed(),
                                                      select.getColumnsLimit()));
            }
        }

        List<Row> Rows = StorageProxy.read(commands, select.getConsistencyLevel());
		 
		 IPartitioner<?> p = StorageService.getPartitioner();
		 AbstractType<?> keyType = Schema.instance.getCFMetaData(metadata.ksName, select.getColumnFamily()).getKeyValidator();
		 ByteBuffer startKeyBytes = (select.getKeyStart() != null)
		                            ? select.getKeyStart().getByteBuffer(keyType,variables)
		                            : null;
		 ByteBuffer finishKeyBytes = (select.getKeyFinish() != null)
		                             ? select.getKeyFinish().getByteBuffer(keyType,variables)
		                             : null;
		 RowPosition startKey = RowPosition.forKey(startKeyBytes, p), finishKey = RowPosition.forKey(finishKeyBytes, p);
		 if (startKey.compareTo(finishKey) > 0 && !finishKey.isMinimum(p))
		 {
		     if (p instanceof RandomPartitioner)
		         throw new InvalidRequestException("Start key sorts after end key. This is not allowed; you probably should not specify end key at all, under RandomPartitioner");
		     else
		         throw new InvalidRequestException("Start key must sort before (or equal to) finish key in your partitioner!");
		 }
		 AbstractBounds<RowPosition> bounds = new Bounds<RowPosition>(startKey, finishKey);
		 IDiskAtomFilter columnFilter = filterFromSelect(select, metadata, variables);
		 validateFilter(metadata, columnFilter);
		 List<Relation> columnRelations = select.getColumnRelations();
		 List<IndexExpression> expressions = new ArrayList<IndexExpression>(columnRelations.size());
		 for (Relation columnRelation : columnRelations)
		 {
		     // Left and right side of relational expression encoded according to comparator/validator.
		     ByteBuffer entity = columnRelation.getEntity().getByteBuffer(metadata.comparator, variables);
		     ByteBuffer value = columnRelation.getValue().getByteBuffer(select.getValueValidator(metadata.ksName, entity), variables);

		     expressions.add(new IndexExpression(entity,
		                                         IndexOperator.valueOf(columnRelation.operator().toString()),
		                                         value));
		 }
		 int limit = select.isKeyRange() && select.getKeyStart() != null
		           ? select.getNumRecords() + 1
		           : select.getNumRecords();
		 
		 RangeSliceCommand command = new RangeSliceCommand(metadata.ksName,
		         select.getColumnFamily(),
		         null,
		         columnFilter,
		         bounds,
		         expressions,
		         limit);
		 try {
			Rows = StorageProxy.filterRows(Rows, command, select.getConsistencyLevel(), 0);
		} catch (Exception e) {
			System.out.println("asdf");
		}
		return Rows;
    }
    

    private static List<org.apache.cassandra.db.Row> getSliceForSpecializedAggregate(CFMetaData metadata, SelectStatement select, List<ByteBuffer> variables, int aggregationType)
    	    throws InvalidRequestException, ReadTimeoutException, UnavailableException, IsBootstrappingException
    {
        QueryPath queryPath = new QueryPath(select.getColumnFamily());
        List<ReadCommand> commands = new ArrayList<ReadCommand>();

        // ...of a list of column names
        if (!select.isColumnRange())
        {
            Collection<ByteBuffer> columnNames = getColumnNames(select, metadata, variables);
            validateColumnNames(columnNames);

            for (Term rawKey: select.getKeys())
            {
                ByteBuffer key = rawKey.getByteBuffer(metadata.getKeyValidator(),variables);

                validateKey(key);
                commands.add(new SliceByNamesReadCommand(metadata.ksName, key, queryPath, columnNames));
            }
        }
        // ...a range (slice) of column names
        else
        {
            AbstractType<?> comparator = select.getComparator(metadata.ksName);
            ByteBuffer start = select.getColumnStart().getByteBuffer(comparator,variables);
            ByteBuffer finish = select.getColumnFinish().getByteBuffer(comparator,variables);

            for (Term rawKey : select.getKeys())
            {
                ByteBuffer key = rawKey.getByteBuffer(metadata.getKeyValidator(),variables);

                validateKey(key);
                validateSliceFilter(metadata, start, finish, select.isColumnsReversed());
                commands.add(new SliceFromReadCommand(metadata.ksName,
                                                      key,
                                                      queryPath,
                                                      start,
                                                      finish,
                                                      select.isColumnsReversed(),
                                                      select.getColumnsLimit()));
            }
        }

        List<Row> Rows = StorageProxy.read(commands, select.getConsistencyLevel());
		 
		 IPartitioner<?> p = StorageService.getPartitioner();
		 AbstractType<?> keyType = Schema.instance.getCFMetaData(metadata.ksName, select.getColumnFamily()).getKeyValidator();
		 ByteBuffer startKeyBytes = (select.getKeyStart() != null)
		                            ? select.getKeyStart().getByteBuffer(keyType,variables)
		                            : null;
		 ByteBuffer finishKeyBytes = (select.getKeyFinish() != null)
		                             ? select.getKeyFinish().getByteBuffer(keyType,variables)
		                             : null;
		 RowPosition startKey = RowPosition.forKey(startKeyBytes, p), finishKey = RowPosition.forKey(finishKeyBytes, p);
		 if (startKey.compareTo(finishKey) > 0 && !finishKey.isMinimum(p))
		 {
		     if (p instanceof RandomPartitioner)
		         throw new InvalidRequestException("Start key sorts after end key. This is not allowed; you probably should not specify end key at all, under RandomPartitioner");
		     else
		         throw new InvalidRequestException("Start key must sort before (or equal to) finish key in your partitioner!");
		 }
		 AbstractBounds<RowPosition> bounds = new Bounds<RowPosition>(startKey, finishKey);
		 IDiskAtomFilter columnFilter = filterFromSelect(select, metadata, variables);
		 validateFilter(metadata, columnFilter);
		 List<Relation> columnRelations = select.getColumnRelations();
		 List<IndexExpression> expressions = new ArrayList<IndexExpression>(columnRelations.size());
		 for (Relation columnRelation : columnRelations)
		 {
		     // Left and right side of relational expression encoded according to comparator/validator.
		     ByteBuffer entity = columnRelation.getEntity().getByteBuffer(metadata.comparator, variables);
		     ByteBuffer value = columnRelation.getValue().getByteBuffer(select.getValueValidator(metadata.ksName, entity), variables);

		     expressions.add(new IndexExpression(entity,
		                                         IndexOperator.valueOf(columnRelation.operator().toString()),
		                                         value));
		 }
		 int limit = select.isKeyRange() && select.getKeyStart() != null
		           ? select.getNumRecords() + 1
		           : select.getNumRecords();
		 
		 RangeSliceCommand command = new RangeSliceCommand(metadata.ksName,
		         select.getColumnFamily(),
		         null,
		         columnFilter,
		         bounds,
		         expressions,
		         limit);
		 try {
			Rows = StorageProxy.filterRowsForAggregate(Rows, command, select.getConsistencyLevel(), aggregationType);
		} catch (Exception e) {
			System.out.println("asdf");
		}
		return Rows;
    }
    
    
    private static SortedSet<ByteBuffer> getColumnNames(SelectStatement select, CFMetaData metadata, List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        String keyString = getKeyString(metadata);
        List<Term> selectColumnNames = select.getColumnNames();
        SortedSet<ByteBuffer> columnNames = new TreeSet<ByteBuffer>(metadata.comparator);
        for (Term column : selectColumnNames)
        {
            // skip the key for the slice op; we'll add it to the resultset in extractThriftColumns
            if (!column.getText().equalsIgnoreCase(keyString))
                columnNames.add(column.getByteBuffer(metadata.comparator,variables));
        }
        return columnNames;
    }

    private static List<org.apache.cassandra.db.Row> multiRangeSlice(CFMetaData metadata, SelectStatement select, List<ByteBuffer> variables)
    throws ReadTimeoutException, UnavailableException, InvalidRequestException
    {
        IPartitioner<?> p = StorageService.getPartitioner();
        List<org.apache.cassandra.db.Row> rows = null;
        AbstractType<?> keyType = Schema.instance.getCFMetaData(metadata.ksName, select.getColumnFamily()).getKeyValidator();

        ByteBuffer startKeyBytes = (select.getKeyStart() != null)
                                   ? select.getKeyStart().getByteBuffer(keyType,variables)
                                   : null;

        ByteBuffer finishKeyBytes = (select.getKeyFinish() != null)
                                    ? select.getKeyFinish().getByteBuffer(keyType,variables)
                                    : null;

        RowPosition startKey = RowPosition.forKey(startKeyBytes, p), finishKey = RowPosition.forKey(finishKeyBytes, p);
        if (startKey.compareTo(finishKey) > 0 && !finishKey.isMinimum(p))
        {
            if (p instanceof RandomPartitioner)
                throw new InvalidRequestException("Start key sorts after end key. This is not allowed; you probably should not specify end key at all, under RandomPartitioner");
            else
                throw new InvalidRequestException("Start key must sort before (or equal to) finish key in your partitioner!");
        }
        AbstractBounds<RowPosition> bounds = new Bounds<RowPosition>(startKey, finishKey);

        IDiskAtomFilter columnFilter = filterFromSelect(select, metadata, variables);
        validateFilter(metadata, columnFilter);

        List<Relation> columnRelations = select.getColumnRelations();
        List<IndexExpression> expressions = new ArrayList<IndexExpression>(columnRelations.size());
        for (Relation columnRelation : columnRelations)
        {
            // Left and right side of relational expression encoded according to comparator/validator.
            ByteBuffer entity = columnRelation.getEntity().getByteBuffer(metadata.comparator, variables);
            ByteBuffer value = columnRelation.getValue().getByteBuffer(select.getValueValidator(metadata.ksName, entity), variables);

            expressions.add(new IndexExpression(entity,
                                                IndexOperator.valueOf(columnRelation.operator().toString()),
                                                value));
        }

        int limit = select.isKeyRange() && select.getKeyStart() != null
                  ? select.getNumRecords() + 1
                  : select.getNumRecords();

		rows = StorageProxy.getRangeSlice(new RangeSliceCommand(metadata.ksName, select.getColumnFamily(),
		                                                  null, columnFilter, bounds, expressions, limit),
		                                                  select.getConsistencyLevel());
		
        // if start key was set and relation was "greater than"
        if (select.getKeyStart() != null && !select.includeStartKey() && !rows.isEmpty())
        {
            if (rows.get(0).key.key.equals(startKeyBytes))
                rows.remove(0);
        }

        // if finish key was set and relation was "less than"
        if (select.getKeyFinish() != null && !select.includeFinishKey() && !rows.isEmpty())
        {
            int lastIndex = rows.size() - 1;
            if (rows.get(lastIndex).key.key.equals(finishKeyBytes))
                rows.remove(lastIndex);
        }

        return rows.subList(0, select.getNumRecords() < rows.size() ? select.getNumRecords() : rows.size());
    }

    private static List<org.apache.cassandra.db.Row> multiRangeSliceForAggretate(CFMetaData metadata, SelectStatement select, List<ByteBuffer> variables, int aggregationType)
    	    throws ReadTimeoutException, UnavailableException, InvalidRequestException
    	    {
    	        IPartitioner<?> p = StorageService.getPartitioner();
    	        List<org.apache.cassandra.db.Row> rows = null;
    	        AbstractType<?> keyType = Schema.instance.getCFMetaData(metadata.ksName, select.getColumnFamily()).getKeyValidator();

    	        ByteBuffer startKeyBytes = (select.getKeyStart() != null)
    	                                   ? select.getKeyStart().getByteBuffer(keyType,variables)
    	                                   : null;

    	        ByteBuffer finishKeyBytes = (select.getKeyFinish() != null)
    	                                    ? select.getKeyFinish().getByteBuffer(keyType,variables)
    	                                    : null;

    	        RowPosition startKey = RowPosition.forKey(startKeyBytes, p), finishKey = RowPosition.forKey(finishKeyBytes, p);
    	        if (startKey.compareTo(finishKey) > 0 && !finishKey.isMinimum(p))
    	        {
    	            if (p instanceof RandomPartitioner)
    	                throw new InvalidRequestException("Start key sorts after end key. This is not allowed; you probably should not specify end key at all, under RandomPartitioner");
    	            else
    	                throw new InvalidRequestException("Start key must sort before (or equal to) finish key in your partitioner!");
    	        }
    	        AbstractBounds<RowPosition> bounds = new Bounds<RowPosition>(startKey, finishKey);

    	        IDiskAtomFilter columnFilter = filterFromSelect(select, metadata, variables);
    	        validateFilter(metadata, columnFilter);

    	        List<Relation> columnRelations = select.getColumnRelations();
    	        List<IndexExpression> expressions = new ArrayList<IndexExpression>(columnRelations.size());
    	        for (Relation columnRelation : columnRelations)
    	        {
    	            // Left and right side of relational expression encoded according to comparator/validator.
    	            ByteBuffer entity = columnRelation.getEntity().getByteBuffer(metadata.comparator, variables);
    	            ByteBuffer value = columnRelation.getValue().getByteBuffer(select.getValueValidator(metadata.ksName, entity), variables);

    	            expressions.add(new IndexExpression(entity,
    	                                                IndexOperator.valueOf(columnRelation.operator().toString()),
    	                                                value));
    	        }

    	        int limit = select.isKeyRange() && select.getKeyStart() != null
    	                  ? select.getNumRecords() + 1
    	                  : select.getNumRecords();

    			rows = StorageProxy.getRangeSliceForAggregate(new RangeSliceCommand(metadata.ksName, select.getColumnFamily(),
    			                                                  null, columnFilter, bounds, expressions, limit),
    			                                                  select.getConsistencyLevel(), aggregationType);
    			
    	        // if start key was set and relation was "greater than"
    	        if (select.getKeyStart() != null && !select.includeStartKey() && !rows.isEmpty())
    	        {
    	            if (rows.get(0).key.key.equals(startKeyBytes))
    	                rows.remove(0);
    	        }

    	        // if finish key was set and relation was "less than"
    	        if (select.getKeyFinish() != null && !select.includeFinishKey() && !rows.isEmpty())
    	        {
    	            int lastIndex = rows.size() - 1;
    	            if (rows.get(lastIndex).key.key.equals(finishKeyBytes))
    	                rows.remove(lastIndex);
    	        }

    	        return rows.subList(0, select.getNumRecords() < rows.size() ? select.getNumRecords() : rows.size());
    	    }

    
    private static List<org.apache.cassandra.db.Row> getSliceForSpecializedUpdate(CFMetaData metadata, SelectStatement select, List<ByteBuffer> variables,
    	    List<IMutation> rowMutations, UpdateStatement update, ThriftClientState clientstate)
    		throws InvalidRequestException, ReadTimeoutException, UnavailableException, IsBootstrappingException
    	    {
    	        QueryPath queryPath = new QueryPath(select.getColumnFamily());
    	        List<ReadCommand> commands = new ArrayList<ReadCommand>();

    	        // ...of a list of column names
    	        if (!select.isColumnRange())
    	        {
    	            Collection<ByteBuffer> columnNames = getColumnNames(select, metadata, variables);
    	            validateColumnNames(columnNames);

    	            for (Term rawKey: select.getKeys())
    	            {
    	                ByteBuffer key = rawKey.getByteBuffer(metadata.getKeyValidator(),variables);

    	                validateKey(key);
    	                commands.add(new SliceByNamesReadCommand(metadata.ksName, key, queryPath, columnNames));
    	            }
    	        }
    	        // ...a range (slice) of column names
    	        else
    	        {
    	            AbstractType<?> comparator = select.getComparator(metadata.ksName);
    	            ByteBuffer start = select.getColumnStart().getByteBuffer(comparator,variables);
    	            ByteBuffer finish = select.getColumnFinish().getByteBuffer(comparator,variables);

    	            for (Term rawKey : select.getKeys())
    	            {
    	                ByteBuffer key = rawKey.getByteBuffer(metadata.getKeyValidator(),variables);

    	                validateKey(key);
    	                validateSliceFilter(metadata, start, finish, select.isColumnsReversed());
    	                commands.add(new SliceFromReadCommand(metadata.ksName,
    	                                                      key,
    	                                                      queryPath,
    	                                                      start,
    	                                                      finish,
    	                                                      select.isColumnsReversed(),
    	                                                      select.getColumnsLimit()));
    	            }
    	        }

    	        List<Row> Rows = StorageProxy.read(commands, select.getConsistencyLevel());
				 
				 IPartitioner<?> p = StorageService.getPartitioner();
				 AbstractType<?> keyType = Schema.instance.getCFMetaData(metadata.ksName, select.getColumnFamily()).getKeyValidator();
				 ByteBuffer startKeyBytes = (select.getKeyStart() != null)
				                            ? select.getKeyStart().getByteBuffer(keyType,variables)
				                            : null;
				 ByteBuffer finishKeyBytes = (select.getKeyFinish() != null)
				                             ? select.getKeyFinish().getByteBuffer(keyType,variables)
				                             : null;
				 RowPosition startKey = RowPosition.forKey(startKeyBytes, p), finishKey = RowPosition.forKey(finishKeyBytes, p);
				 if (startKey.compareTo(finishKey) > 0 && !finishKey.isMinimum(p))
				 {
				     if (p instanceof RandomPartitioner)
				         throw new InvalidRequestException("Start key sorts after end key. This is not allowed; you probably should not specify end key at all, under RandomPartitioner");
				     else
				         throw new InvalidRequestException("Start key must sort before (or equal to) finish key in your partitioner!");
				 }
				 AbstractBounds<RowPosition> bounds = new Bounds<RowPosition>(startKey, finishKey);
				 IDiskAtomFilter columnFilter = filterFromSelect(select, metadata, variables);
				 validateFilter(metadata, columnFilter);
				 List<Relation> columnRelations = select.getColumnRelations();
				 List<IndexExpression> expressions = new ArrayList<IndexExpression>(columnRelations.size());
				 for (Relation columnRelation : columnRelations)
				 {
				     // Left and right side of relational expression encoded according to comparator/validator.
				     ByteBuffer entity = columnRelation.getEntity().getByteBuffer(metadata.comparator, variables);
				     ByteBuffer value = columnRelation.getValue().getByteBuffer(select.getValueValidator(metadata.ksName, entity), variables);

				     expressions.add(new IndexExpression(entity,
				                                         IndexOperator.valueOf(columnRelation.operator().toString()),
				                                         value));
				 }
				 int limit = select.isKeyRange() && select.getKeyStart() != null
				           ? select.getNumRecords() + 1
				           : select.getNumRecords();
				 
				 RangeSliceCommand command = new RangeSliceCommand(metadata.ksName,
				         select.getColumnFamily(),
				         null,
				         columnFilter,
				         bounds,
				         expressions,
				         limit);
				 try {
					Rows = StorageProxy.filterRowsForUpdate(Rows, command, update.getConsistencyLevel(), rowMutations, update, clientstate);
				} catch (Exception e) {
					System.out.println("asdf");
				}
				return Rows;
    	    }
    
    private static List<org.apache.cassandra.db.Row> getSliceForSpecializedClear(CFMetaData metadata, SelectStatement select, List<ByteBuffer> variables,
    	    List<IMutation> rowMutations, UpdateStatement update, ThriftClientState clientstate, char invalidchar)
    		throws InvalidRequestException, ReadTimeoutException, UnavailableException, IsBootstrappingException
    	    {
    	        QueryPath queryPath = new QueryPath(select.getColumnFamily());
    	        List<ReadCommand> commands = new ArrayList<ReadCommand>();

    	        // ...of a list of column names
    	        if (!select.isColumnRange())
    	        {
    	            Collection<ByteBuffer> columnNames = getColumnNames(select, metadata, variables);
    	            validateColumnNames(columnNames);

    	            for (Term rawKey: select.getKeys())
    	            {
    	                ByteBuffer key = rawKey.getByteBuffer(metadata.getKeyValidator(),variables);

    	                validateKey(key);
    	                commands.add(new SliceByNamesReadCommand(metadata.ksName, key, queryPath, columnNames));
    	            }
    	        }
    	        // ...a range (slice) of column names
    	        else
    	        {
    	            AbstractType<?> comparator = select.getComparator(metadata.ksName);
    	            ByteBuffer start = select.getColumnStart().getByteBuffer(comparator,variables);
    	            ByteBuffer finish = select.getColumnFinish().getByteBuffer(comparator,variables);

    	            for (Term rawKey : select.getKeys())
    	            {
    	                ByteBuffer key = rawKey.getByteBuffer(metadata.getKeyValidator(),variables);

    	                validateKey(key);
    	                validateSliceFilter(metadata, start, finish, select.isColumnsReversed());
    	                commands.add(new SliceFromReadCommand(metadata.ksName,
    	                                                      key,
    	                                                      queryPath,
    	                                                      start,
    	                                                      finish,
    	                                                      select.isColumnsReversed(),
    	                                                      select.getColumnsLimit()));
    	            }
    	        }

    	        List<Row> Rows = StorageProxy.read(commands, select.getConsistencyLevel());
				 
				 IPartitioner<?> p = StorageService.getPartitioner();
				 AbstractType<?> keyType = Schema.instance.getCFMetaData(metadata.ksName, select.getColumnFamily()).getKeyValidator();
				 ByteBuffer startKeyBytes = (select.getKeyStart() != null)
				                            ? select.getKeyStart().getByteBuffer(keyType,variables)
				                            : null;
				 ByteBuffer finishKeyBytes = (select.getKeyFinish() != null)
				                             ? select.getKeyFinish().getByteBuffer(keyType,variables)
				                             : null;
				 RowPosition startKey = RowPosition.forKey(startKeyBytes, p), finishKey = RowPosition.forKey(finishKeyBytes, p);
				 if (startKey.compareTo(finishKey) > 0 && !finishKey.isMinimum(p))
				 {
				     if (p instanceof RandomPartitioner)
				         throw new InvalidRequestException("Start key sorts after end key. This is not allowed; you probably should not specify end key at all, under RandomPartitioner");
				     else
				         throw new InvalidRequestException("Start key must sort before (or equal to) finish key in your partitioner!");
				 }
				 AbstractBounds<RowPosition> bounds = new Bounds<RowPosition>(startKey, finishKey);
				 IDiskAtomFilter columnFilter = filterFromSelect(select, metadata, variables);
				 validateFilter(metadata, columnFilter);
				 List<Relation> columnRelations = select.getColumnRelations();
				 List<IndexExpression> expressions = new ArrayList<IndexExpression>(columnRelations.size());
				 for (Relation columnRelation : columnRelations)
				 {
				     // Left and right side of relational expression encoded according to comparator/validator.
				     ByteBuffer entity = columnRelation.getEntity().getByteBuffer(metadata.comparator, variables);
				     ByteBuffer value = columnRelation.getValue().getByteBuffer(select.getValueValidator(metadata.ksName, entity), variables);

				     expressions.add(new IndexExpression(entity,
				                                         IndexOperator.valueOf(columnRelation.operator().toString()),
				                                         value));
				 }
				 int limit = select.isKeyRange() && select.getKeyStart() != null
				           ? select.getNumRecords() + 1
				           : select.getNumRecords();
				 
				 RangeSliceCommand command = new RangeSliceCommand(metadata.ksName,
				         select.getColumnFamily(),
				         null,
				         columnFilter,
				         bounds,
				         expressions,
				         limit);
				 try {
					Rows = StorageProxy.filterRowsForClear(Rows, command, update.getConsistencyLevel(), rowMutations, update, clientstate, invalidchar);
				} catch (Exception e) {
					System.out.println("asdf");
				}
				return Rows;
    	    }
    
    private static List<org.apache.cassandra.db.Row> getSliceForSpecializedDelete(CFMetaData metadata, SelectStatement select, List<ByteBuffer> variables,
    		List<IMutation> deleteMutations, DeleteStatement delete, ThriftClientState clientstate)
    		throws InvalidRequestException, ReadTimeoutException, UnavailableException, IsBootstrappingException
    	    {
    	        QueryPath queryPath = new QueryPath(select.getColumnFamily());
    	        List<ReadCommand> commands = new ArrayList<ReadCommand>();

    	        // ...of a list of column names
    	        if (!select.isColumnRange())
    	        {
    	            Collection<ByteBuffer> columnNames = getColumnNames(select, metadata, variables);
    	            validateColumnNames(columnNames);

    	            for (Term rawKey: select.getKeys())
    	            {
    	                ByteBuffer key = rawKey.getByteBuffer(metadata.getKeyValidator(),variables);

    	                validateKey(key);
    	                commands.add(new SliceByNamesReadCommand(metadata.ksName, key, queryPath, columnNames));
    	            }
    	        }
    	        // ...a range (slice) of column names
    	        else
    	        {
    	            AbstractType<?> comparator = select.getComparator(metadata.ksName);
    	            ByteBuffer start = select.getColumnStart().getByteBuffer(comparator,variables);
    	            ByteBuffer finish = select.getColumnFinish().getByteBuffer(comparator,variables);

    	            for (Term rawKey : select.getKeys())
    	            {
    	                ByteBuffer key = rawKey.getByteBuffer(metadata.getKeyValidator(),variables);

    	                validateKey(key);
    	                validateSliceFilter(metadata, start, finish, select.isColumnsReversed());
    	                commands.add(new SliceFromReadCommand(metadata.ksName,
    	                                                      key,
    	                                                      queryPath,
    	                                                      start,
    	                                                      finish,
    	                                                      select.isColumnsReversed(),
    	                                                      select.getColumnsLimit()));
    	            }
    	        }

    	        List<Row> Rows = StorageProxy.read(commands, select.getConsistencyLevel());
				 
				 IPartitioner<?> p = StorageService.getPartitioner();
				 AbstractType<?> keyType = Schema.instance.getCFMetaData(metadata.ksName, select.getColumnFamily()).getKeyValidator();
				 ByteBuffer startKeyBytes = (select.getKeyStart() != null)
				                            ? select.getKeyStart().getByteBuffer(keyType,variables)
				                            : null;
				 ByteBuffer finishKeyBytes = (select.getKeyFinish() != null)
				                             ? select.getKeyFinish().getByteBuffer(keyType,variables)
				                             : null;
				 RowPosition startKey = RowPosition.forKey(startKeyBytes, p), finishKey = RowPosition.forKey(finishKeyBytes, p);
				 if (startKey.compareTo(finishKey) > 0 && !finishKey.isMinimum(p))
				 {
				     if (p instanceof RandomPartitioner)
				         throw new InvalidRequestException("Start key sorts after end key. This is not allowed; you probably should not specify end key at all, under RandomPartitioner");
				     else
				         throw new InvalidRequestException("Start key must sort before (or equal to) finish key in your partitioner!");
				 }
				 AbstractBounds<RowPosition> bounds = new Bounds<RowPosition>(startKey, finishKey);
				 IDiskAtomFilter columnFilter = filterFromSelect(select, metadata, variables);
				 validateFilter(metadata, columnFilter);
				 List<Relation> columnRelations = select.getColumnRelations();
				 List<IndexExpression> expressions = new ArrayList<IndexExpression>(columnRelations.size());
				 for (Relation columnRelation : columnRelations)
				 {
				     // Left and right side of relational expression encoded according to comparator/validator.
				     ByteBuffer entity = columnRelation.getEntity().getByteBuffer(metadata.comparator, variables);
				     ByteBuffer value = columnRelation.getValue().getByteBuffer(select.getValueValidator(metadata.ksName, entity), variables);

				     expressions.add(new IndexExpression(entity,
				                                         IndexOperator.valueOf(columnRelation.operator().toString()),
				                                         value));
				 }
				 int limit = select.isKeyRange() && select.getKeyStart() != null
				           ? select.getNumRecords() + 1
				           : select.getNumRecords();
				 
				 RangeSliceCommand command = new RangeSliceCommand(metadata.ksName,
				         select.getColumnFamily(),
				         null,
				         columnFilter,
				         bounds,
				         expressions,
				         limit);
				 try {
					Rows = StorageProxy.filterRowsForDelete(Rows, command, delete.getConsistencyLevel(), deleteMutations, delete, clientstate);
				} catch (Exception e) {
					System.out.println("asdf");
				}
				return Rows;
    	    }
    
    private static List<org.apache.cassandra.db.Row> multiRangeSliceForUpdate(CFMetaData metadata, SelectStatement select, List<ByteBuffer> variables,
    		List<IMutation> rowMutations, UpdateStatement update, ThriftClientState clientstate)
    	    throws ReadTimeoutException, UnavailableException, InvalidRequestException
    	    {
    	        List<org.apache.cassandra.db.Row> rows;
    	        IPartitioner<?> p = StorageService.getPartitioner();

    	        AbstractType<?> keyType = Schema.instance.getCFMetaData(metadata.ksName, select.getColumnFamily()).getKeyValidator();

    	        ByteBuffer startKeyBytes = (select.getKeyStart() != null)
    	                                   ? select.getKeyStart().getByteBuffer(keyType,variables)
    	                                   : null;

    	        ByteBuffer finishKeyBytes = (select.getKeyFinish() != null)
    	                                    ? select.getKeyFinish().getByteBuffer(keyType,variables)
    	                                    : null;

    	        RowPosition startKey = RowPosition.forKey(startKeyBytes, p), finishKey = RowPosition.forKey(finishKeyBytes, p);
    	        if (startKey.compareTo(finishKey) > 0 && !finishKey.isMinimum(p))
    	        {
    	            if (p instanceof RandomPartitioner)
    	                throw new InvalidRequestException("Start key sorts after end key. This is not allowed; you probably should not specify end key at all, under RandomPartitioner");
    	            else
    	                throw new InvalidRequestException("Start key must sort before (or equal to) finish key in your partitioner!");
    	        }
    	        AbstractBounds<RowPosition> bounds = new Bounds<RowPosition>(startKey, finishKey);

    	        IDiskAtomFilter columnFilter = filterFromSelect(select, metadata, variables);
    	        validateFilter(metadata, columnFilter);

    	        List<Relation> columnRelations = select.getColumnRelations();
    	        List<IndexExpression> expressions = new ArrayList<IndexExpression>(columnRelations.size());
    	        for (Relation columnRelation : columnRelations)
    	        {
    	            // Left and right side of relational expression encoded according to comparator/validator.
    	            ByteBuffer entity = columnRelation.getEntity().getByteBuffer(metadata.comparator, variables);
    	            ByteBuffer value = columnRelation.getValue().getByteBuffer(select.getValueValidator(metadata.ksName, entity), variables);

    	            expressions.add(new IndexExpression(entity,
    	                                                IndexOperator.valueOf(columnRelation.operator().toString()),
    	                                                value));
    	        }

    	        int limit = select.isKeyRange() && select.getKeyStart() != null
    	                  ? select.getNumRecords() + 1
    	                  : select.getNumRecords();

    	        try
    	        {
    	            rows = StorageProxy.getRangeSliceForUpdate(new RangeSliceCommand(metadata.ksName,
    	                                                                    select.getColumnFamily(),
    	                                                                    null,
    	                                                                    columnFilter,
    	                                                                    bounds,
    	                                                                    expressions,
    	                                                                    limit),
    	                                                                    select.getConsistencyLevel(), 
    	                                                                    rowMutations, 
    	                                                                    update,
    	                                                                    clientstate);
    	        }
    	        catch (IOException e)
    	        {
    	            throw new RuntimeException(e);
    	        }

    	        // if start key was set and relation was "greater than"
    	        if (select.getKeyStart() != null && !select.includeStartKey() && !rows.isEmpty())
    	        {
    	            if (rows.get(0).key.key.equals(startKeyBytes))
    	                rows.remove(0);
    	        }

    	        // if finish key was set and relation was "less than"
    	        if (select.getKeyFinish() != null && !select.includeFinishKey() && !rows.isEmpty())
    	        {
    	            int lastIndex = rows.size() - 1;
    	            if (rows.get(lastIndex).key.key.equals(finishKeyBytes))
    	                rows.remove(lastIndex);
    	        }

    	        return rows.subList(0, select.getNumRecords() < rows.size() ? select.getNumRecords() : rows.size());
    	    }


    private static List<org.apache.cassandra.db.Row> multiRangeSliceForClear(CFMetaData metadata, SelectStatement select, List<ByteBuffer> variables,
    		List<IMutation> rowMutations, UpdateStatement update, ThriftClientState clientstate, char invalidChar)
    	    throws ReadTimeoutException, UnavailableException, InvalidRequestException
    	    {
    	        List<org.apache.cassandra.db.Row> rows;
    	        IPartitioner<?> p = StorageService.getPartitioner();

    	        AbstractType<?> keyType = Schema.instance.getCFMetaData(metadata.ksName, select.getColumnFamily()).getKeyValidator();

    	        ByteBuffer startKeyBytes = (select.getKeyStart() != null)
    	                                   ? select.getKeyStart().getByteBuffer(keyType,variables)
    	                                   : null;

    	        ByteBuffer finishKeyBytes = (select.getKeyFinish() != null)
    	                                    ? select.getKeyFinish().getByteBuffer(keyType,variables)
    	                                    : null;

    	        RowPosition startKey = RowPosition.forKey(startKeyBytes, p), finishKey = RowPosition.forKey(finishKeyBytes, p);
    	        if (startKey.compareTo(finishKey) > 0 && !finishKey.isMinimum(p))
    	        {
    	            if (p instanceof RandomPartitioner)
    	                throw new InvalidRequestException("Start key sorts after end key. This is not allowed; you probably should not specify end key at all, under RandomPartitioner");
    	            else
    	                throw new InvalidRequestException("Start key must sort before (or equal to) finish key in your partitioner!");
    	        }
    	        AbstractBounds<RowPosition> bounds = new Bounds<RowPosition>(startKey, finishKey);

    	        IDiskAtomFilter columnFilter = filterFromSelect(select, metadata, variables);
    	        validateFilter(metadata, columnFilter);

    	        List<Relation> columnRelations = select.getColumnRelations();
    	        List<IndexExpression> expressions = new ArrayList<IndexExpression>(columnRelations.size());
    	        for (Relation columnRelation : columnRelations)
    	        {
    	            // Left and right side of relational expression encoded according to comparator/validator.
    	            ByteBuffer entity = columnRelation.getEntity().getByteBuffer(metadata.comparator, variables);
    	            ByteBuffer value = columnRelation.getValue().getByteBuffer(select.getValueValidator(metadata.ksName, entity), variables);

    	            expressions.add(new IndexExpression(entity,
    	                                                IndexOperator.valueOf(columnRelation.operator().toString()),
    	                                                value));
    	        }

    	        int limit = select.isKeyRange() && select.getKeyStart() != null
    	                  ? select.getNumRecords() + 1
    	                  : select.getNumRecords();

    	        try
    	        {
    	            rows = StorageProxy.getRangeSliceForClear(new RangeSliceCommand(metadata.ksName,
    	                                                                    select.getColumnFamily(),
    	                                                                    null,
    	                                                                    columnFilter,
    	                                                                    bounds,
    	                                                                    expressions,
    	                                                                    limit),
    	                                                                    select.getConsistencyLevel(), 
    	                                                                    rowMutations, 
    	                                                                    update,
    	                                                                    clientstate, 
    	                                                                    invalidChar);
    	        }
    	        catch (IOException e)
    	        {
    	            throw new RuntimeException(e);
    	        }

    	        // if start key was set and relation was "greater than"
    	        if (select.getKeyStart() != null && !select.includeStartKey() && !rows.isEmpty())
    	        {
    	            if (rows.get(0).key.key.equals(startKeyBytes))
    	                rows.remove(0);
    	        }

    	        // if finish key was set and relation was "less than"
    	        if (select.getKeyFinish() != null && !select.includeFinishKey() && !rows.isEmpty())
    	        {
    	            int lastIndex = rows.size() - 1;
    	            if (rows.get(lastIndex).key.key.equals(finishKeyBytes))
    	                rows.remove(lastIndex);
    	        }

    	        return rows.subList(0, select.getNumRecords() < rows.size() ? select.getNumRecords() : rows.size());
    	    }

    
    private static List<org.apache.cassandra.db.Row> multiRangeSliceForDelete(CFMetaData metadata, SelectStatement select, List<ByteBuffer> variables,
    		List<IMutation> deleteMutations, DeleteStatement delete, ThriftClientState clientstate)
    	    throws ReadTimeoutException, UnavailableException, InvalidRequestException
    	    {
    	        List<org.apache.cassandra.db.Row> rows;
    	        IPartitioner<?> p = StorageService.getPartitioner();

    	        AbstractType<?> keyType = Schema.instance.getCFMetaData(metadata.ksName, select.getColumnFamily()).getKeyValidator();

    	        ByteBuffer startKeyBytes = (select.getKeyStart() != null)
    	                                   ? select.getKeyStart().getByteBuffer(keyType,variables)
    	                                   : null;

    	        ByteBuffer finishKeyBytes = (select.getKeyFinish() != null)
    	                                    ? select.getKeyFinish().getByteBuffer(keyType,variables)
    	                                    : null;

    	        RowPosition startKey = RowPosition.forKey(startKeyBytes, p), finishKey = RowPosition.forKey(finishKeyBytes, p);
    	        if (startKey.compareTo(finishKey) > 0 && !finishKey.isMinimum(p))
    	        {
    	            if (p instanceof RandomPartitioner)
    	                throw new InvalidRequestException("Start key sorts after end key. This is not allowed; you probably should not specify end key at all, under RandomPartitioner");
    	            else
    	                throw new InvalidRequestException("Start key must sort before (or equal to) finish key in your partitioner!");
    	        }
    	        AbstractBounds<RowPosition> bounds = new Bounds<RowPosition>(startKey, finishKey);

    	        IDiskAtomFilter columnFilter = filterFromSelect(select, metadata, variables);
    	        validateFilter(metadata, columnFilter);

    	        List<Relation> columnRelations = select.getColumnRelations();
    	        List<IndexExpression> expressions = new ArrayList<IndexExpression>(columnRelations.size());
    	        for (Relation columnRelation : columnRelations)
    	        {
    	            // Left and right side of relational expression encoded according to comparator/validator.
    	            ByteBuffer entity = columnRelation.getEntity().getByteBuffer(metadata.comparator, variables);
    	            ByteBuffer value = columnRelation.getValue().getByteBuffer(select.getValueValidator(metadata.ksName, entity), variables);

    	            expressions.add(new IndexExpression(entity,
    	                                                IndexOperator.valueOf(columnRelation.operator().toString()),
    	                                                value));
    	        }

    	        int limit = select.isKeyRange() && select.getKeyStart() != null
    	                  ? select.getNumRecords() + 1
    	                  : select.getNumRecords();

    	        try
    	        {
    	            rows = StorageProxy.getRangeSliceForDelete(new RangeSliceCommand(metadata.ksName,
    	                                                                    select.getColumnFamily(),
    	                                                                    null,
    	                                                                    columnFilter,
    	                                                                    bounds,
    	                                                                    expressions,
    	                                                                    limit),
    	                                                                    select.getConsistencyLevel(), 
    	                                                                    deleteMutations, 
    	                                                                    delete,
    	                                                                    clientstate);
    	        }
    	        catch (IOException e)
    	        {
    	            throw new RuntimeException(e);
    	        }

    	        // if start key was set and relation was "greater than"
    	        if (select.getKeyStart() != null && !select.includeStartKey() && !rows.isEmpty())
    	        {
    	            if (rows.get(0).key.key.equals(startKeyBytes))
    	                rows.remove(0);
    	        }

    	        // if finish key was set and relation was "less than"
    	        if (select.getKeyFinish() != null && !select.includeFinishKey() && !rows.isEmpty())
    	        {
    	            int lastIndex = rows.size() - 1;
    	            if (rows.get(lastIndex).key.key.equals(finishKeyBytes))
    	                rows.remove(lastIndex);
    	        }

    	        return rows.subList(0, select.getNumRecords() < rows.size() ? select.getNumRecords() : rows.size());
    	    }

    
    private static IDiskAtomFilter filterFromSelect(SelectStatement select, CFMetaData metadata, List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        if (select.isColumnRange() || select.getColumnNames().size() == 0)
        {
            return new SliceQueryFilter(select.getColumnStart().getByteBuffer(metadata.comparator, variables),
                                        select.getColumnFinish().getByteBuffer(metadata.comparator, variables),
                                        select.isColumnsReversed(),
                                        select.getColumnsLimit());
        }
        else
        {
            return new NamesQueryFilter(getColumnNames(select, metadata, variables));
        }
    }

    /* Test for SELECT-specific taboos */
    private static void validateSelect(String keyspace, SelectStatement select, List<ByteBuffer> variables) throws InvalidRequestException
    {
        select.getConsistencyLevel().validateForRead(keyspace);

        // Finish key w/o start key (KEY < foo)
        if (!select.isKeyRange() && (select.getKeyFinish() != null))
            throw new InvalidRequestException("Key range clauses must include a start key (i.e. KEY > term)");

        // Key range and by-key(s) combined (KEY > foo AND KEY = bar)
        if (select.isKeyRange() && select.getKeys().size() > 0)
            throw new InvalidRequestException("You cannot combine key range and by-key clauses in a SELECT");

        // Start and finish keys, *and* column relations (KEY > foo AND KEY < bar and name1 = value1).
        if (select.isKeyRange() && (select.getKeyFinish() != null) && (select.getColumnRelations().size() > 0))
            throw new InvalidRequestException("You cannot combine key range and by-column clauses in a SELECT");

        // Can't use more than one KEY =
        if (!select.isMultiKey() && select.getKeys().size() > 1)
            throw new InvalidRequestException("You cannot use more than one KEY = in a SELECT");

        if (select.getColumnRelations().size() > 0)
        {
            AbstractType<?> comparator = select.getComparator(keyspace);
            SecondaryIndexManager idxManager = Table.open(keyspace).getColumnFamilyStore(select.getColumnFamily()).indexManager;
            
            /*
            for (Relation relation : select.getColumnRelations())
            {
                ByteBuffer name = relation.getEntity().getByteBuffer(comparator, variables);
                if ((relation.operator() == RelationType.EQ) && idxManager.indexes(name))
                    return;
            }
            throw new InvalidRequestException("No indexed columns present in by-columns clause with \"equals\" operator"); 
        	*/
            
            //added by xuhao
	        select.setSearchWithoutKey(true);
        }
    }

    public static boolean validateKey(ByteBuffer key) throws InvalidRequestException
    {
    	if (key == null || key.remaining() == 0)
        {
            //throw new InvalidRequestException("Key may not be empty");
        	return false;
        }

        // check that key can be handled by FBUtilities.writeShortByteArray
        if (key.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            //throw new InvalidRequestException("Key length of " + key.remaining() +
                                              //" is longer than maximum of " + FBUtilities.MAX_UNSIGNED_SHORT);
            return false;
        }
        return true;
    }

    public static boolean validateKeyAlias(CFMetaData cfm, String key) throws InvalidRequestException
    {
    	assert key.toUpperCase().equals(key); // should always be uppercased by caller
        String realKeyAlias = bufferToString(cfm.getKeyName()).toUpperCase();
        if (!realKeyAlias.equals(key)) 
        	return false;
        return true;
        //throw new InvalidRequestException(String.format("Expected key '%s' to be present in WHERE clause for '%s'", realKeyAlias, cfm.cfName));
    }

    private static void validateColumnNames(Iterable<ByteBuffer> columns)
    throws InvalidRequestException
    {
        for (ByteBuffer name : columns)
        {
            if (name.remaining() > IColumn.MAX_NAME_LENGTH)
                throw new InvalidRequestException(String.format("column name is too long (%s > %s)",
                                                                name.remaining(),
                                                                IColumn.MAX_NAME_LENGTH));
            if (name.remaining() == 0)
                throw new InvalidRequestException("zero-length column name");
        }
    }

    public static void validateColumnName(ByteBuffer column)
    throws InvalidRequestException
    {
        validateColumnNames(Arrays.asList(column));
    }

    public static void validateColumn(CFMetaData metadata, ByteBuffer name, ByteBuffer value)
    throws InvalidRequestException
    {
        validateColumnName(name);
        AbstractType<?> validator = metadata.getValueValidator(name);

        try
        {
            if (validator != null)
                validator.validate(value);
        }
        catch (MarshalException me)
        {
            throw new InvalidRequestException(String.format("Invalid column value for column (name=%s); %s",
                                                            ByteBufferUtil.bytesToHex(name),
                                                            me.getMessage()));
        }
    }

    private static void validateFilter(CFMetaData metadata, IDiskAtomFilter filter)
    throws InvalidRequestException
    {
        if (filter instanceof SliceQueryFilter)
            validateSliceFilter(metadata, (SliceQueryFilter)filter);
        else
            validateColumnNames(((NamesQueryFilter)filter).columns);
    }

    private static void validateSliceFilter(CFMetaData metadata, SliceQueryFilter range)
    throws InvalidRequestException
    {
        validateSliceFilter(metadata, range.start(), range.finish(), range.reversed);
    }

    private static void validateSliceFilter(CFMetaData metadata, ByteBuffer start, ByteBuffer finish, boolean reversed)
    throws InvalidRequestException
    {
        AbstractType<?> comparator = metadata.getComparatorFor(null);
        Comparator<ByteBuffer> orderedComparator = reversed ? comparator.reverseComparator: comparator;
        if (start.remaining() > 0 && finish.remaining() > 0 && orderedComparator.compare(start, finish) > 0)
            throw new InvalidRequestException("range finish must come after start in traversal order");
    }

    private static Map<String, List<String>> describeSchemaVersions()
    {
        // unreachable hosts don't count towards disagreement
        return Maps.filterKeys(StorageProxy.describeSchemaVersions(),
                               Predicates.not(Predicates.equalTo(StorageProxy.UNREACHABLE)));
    }

    public static String decode(ByteBuffer buffer)
    {
        System.out.println( " buffer= "   +  buffer);
        Charset charset = null ;
        CharsetDecoder decoder = null ;
        CharBuffer charBuffer = null ;
        try 
        {
        	charset = Charset.forName("gb2312");
            decoder = charset.newDecoder();
            charBuffer = decoder.decode(buffer);
            System.out.println("charBuffer= " + charBuffer);
            System.out.println(charBuffer.toString());
            return charBuffer.toString();
        } 
        catch(Exception ex)
        {
            ex.printStackTrace();
            return "";
        } 
    }
    
    public static CqlResult processStatement(CQLStatement statement,ThriftClientState clientState, List<ByteBuffer> variables )
    throws RequestExecutionException, RequestValidationException
    {
        String keyspace = null;

        // Some statements won't have (or don't need) a keyspace (think USE, or CREATE).
        if (statement.type != StatementType.SELECT && StatementType.requiresKeyspace.contains(statement.type))
            keyspace = clientState.getKeyspace();

        CqlResult result = new CqlResult();

        if (logger.isDebugEnabled()) logger.debug("CQL statement type: {}", statement.type.toString());
        CFMetaData metadata;
        switch (statement.type)
        {
            case SELECT:
                SelectStatement select = (SelectStatement)statement.statement;

                final String oldKeyspace = clientState.getRawKeyspace();

                if (select.isSetKeyspace())
                {
                    keyspace = CliUtils.unescapeSQLString(select.getKeyspace());
                    ThriftValidation.validateTable(keyspace);
                }
                else if (oldKeyspace == null)
                    throw new InvalidRequestException("no keyspace has been specified");
                else
                    keyspace = oldKeyspace;

                clientState.hasColumnFamilyAccess(keyspace, select.getColumnFamily(), Permission.SELECT);
                metadata = validateColumnFamily(keyspace, select.getColumnFamily());

                // need to do this in here because we need a CFMD.getKeyName()
                select.extractKeyAliasFromColumns(metadata);

                if (select.getKeys().size() > 0)
                    validateKeyAlias(metadata, select.getKeyAlias());

                validateSelect(keyspace, select, variables);

                List<org.apache.cassandra.db.Row> rows;

                boolean traversal = (((oriQueryString.indexOf("value") != -1) && (oriQueryString.indexOf("\'value\'")) == -1)
                		|| ((oriQueryString.indexOf("column") != -1) && (oriQueryString.indexOf("\'column\'")) == -1)) ? 
                				true : false;
                

                result.type = CqlResultType.ROWS;
                
                //1, max; 2, min; 3, sum; 4, count; 5, average; 6, variance; 7, tercile25; 8, tercile75;
                //max
                if(select.getAggregateType() == 1 || select.getAggregateType() == 2 || select.getAggregateType() == 3
                		|| select.getAggregateType() == 4 || select.getAggregateType() == 5 || select.getAggregateType() == 6)
                {
                	if(select.numOfClauseRelations() > select.getColumnRelations().size())
	                {
	                	rows = getSliceForSpecializedAggregate(metadata, select, variables, select.getAggregateType());
	                }
	                //without key and traversal
	                else
	                {
	                    rows = multiRangeSliceForAggretate(metadata, select, variables, select.getAggregateType());
	                }
                	
                	ByteBuffer countBytes = ByteBufferUtil.bytes("");
                	if(select.getAggregateType() == 1) countBytes = ByteBufferUtil.bytes("Max");
                	if(select.getAggregateType() == 2) countBytes = ByteBufferUtil.bytes("Min");
                	if(select.getAggregateType() == 3) countBytes = ByteBufferUtil.bytes("Sum");
                	if(select.getAggregateType() == 4) countBytes = ByteBufferUtil.bytes("Count Lines");
                	if(select.getAggregateType() == 5) countBytes = ByteBufferUtil.bytes("Average");
                	if(select.getAggregateType() == 6) countBytes = ByteBufferUtil.bytes("Variance");
                	
                    result.schema = new CqlMetadata(Collections.<ByteBuffer, String>emptyMap(),
                                                    Collections.<ByteBuffer, String>emptyMap(),
                                                    "AsciiType",
                                                    "AsciiType");
                    ByteBuffer valueBuffer = rows.get(0).cf.getSortedColumns().iterator().next().value();
                    //String valueString = decode(valueBuffer);
                    List<Column> columns = Collections.singletonList(new Column(countBytes).setValue(valueBuffer));
                    result.rows = Collections.singletonList(new CqlRow(countBytes, columns));
                    return result;
                }
                
                // not aggregate functions
                else{
                	//By-key
	                if (!select.isKeyRange() && (select.getKeys().size() > 0) && !traversal)
	                {
	                    rows = getSlice(metadata, select, variables);
	                }
	                //with key
	                else if(select.numOfClauseRelations() > select.getColumnRelations().size() && traversal)
	                {
	                	rows = getSliceForSpecializedSelect(metadata, select, variables);
	                }
	                //without key and traversal
	                else
	                {
	                    rows = multiRangeSlice(metadata, select, variables);
	                }
                }

                // count resultset is a single column named "count"
                result.type = CqlResultType.ROWS;
                if (select.isCountOperation())
                {
                    validateCountOperation(select);

                    ByteBuffer countBytes = ByteBufferUtil.bytes("count");
                    result.schema = new CqlMetadata(Collections.<ByteBuffer, String>emptyMap(),
                                                    Collections.<ByteBuffer, String>emptyMap(),
                                                    "AsciiType",
                                                    "LongType");
                    List<Column> columns = Collections.singletonList(new Column(countBytes).setValue(ByteBufferUtil.bytes((long) rows.size())));
                    result.rows = Collections.singletonList(new CqlRow(countBytes, columns));
                    return result;
                }

                // otherwise create resultset from query results
                result.schema = new CqlMetadata(new HashMap<ByteBuffer, String>(),
                                                new HashMap<ByteBuffer, String>(),
                                                TypeParser.getShortName(metadata.comparator),
                                                TypeParser.getShortName(metadata.getDefaultValidator()));
                List<CqlRow> cqlRows = new ArrayList<CqlRow>(rows.size());
                for (org.apache.cassandra.db.Row row : rows)
                {
                    List<Column> thriftColumns = new ArrayList<Column>();
                    if (select.isColumnRange())
                    {
                        if (select.isFullWildcard())
                        {
                            // prepend key
                            thriftColumns.add(new Column(metadata.getKeyName()).setValue(row.key.key).setTimestamp(-1));
                            result.schema.name_types.put(metadata.getKeyName(), TypeParser.getShortName(AsciiType.instance));
                            result.schema.value_types.put(metadata.getKeyName(), TypeParser.getShortName(metadata.getKeyValidator()));
                        }

                        // preserve comparator order
                        if (row.cf != null)
                        {
                            for (IColumn c : row.cf.getSortedColumns())
                            {
                                if (c.isMarkedForDelete())
                                    continue;

                                ColumnDefinition cd = metadata.getColumnDefinitionFromColumnName(c.name());
                                if (cd != null)
                                    result.schema.value_types.put(c.name(), TypeParser.getShortName(cd.getValidator()));

                                thriftColumns.add(thriftify(c));
                            }
                        }
                    }
                    else
                    {
                        String keyString = getKeyString(metadata);

                        // order columns in the order they were asked for
                        for (Term term : select.getColumnNames())
                        {
                            if (term.getText().equalsIgnoreCase(keyString))
                            {
                                // preserve case of key as it was requested
                                ByteBuffer requestedKey = ByteBufferUtil.bytes(term.getText());
                                thriftColumns.add(new Column(requestedKey).setValue(row.key.key).setTimestamp(-1));
                                result.schema.name_types.put(requestedKey, TypeParser.getShortName(AsciiType.instance));
                                result.schema.value_types.put(requestedKey, TypeParser.getShortName(metadata.getKeyValidator()));
                                continue;
                            }

                            if (row.cf == null)
                                continue;

                            ByteBuffer name;
                            try
                            {
                                name = term.getByteBuffer(metadata.comparator, variables);
                            }
                            catch (InvalidRequestException e)
                            {
                                throw new AssertionError(e);
                            }

                            ColumnDefinition cd = metadata.getColumnDefinitionFromColumnName(name);
                            if (cd != null)
                                result.schema.value_types.put(name, TypeParser.getShortName(cd.getValidator()));
                            IColumn c = row.cf.getColumn(name);
                            if (c == null || c.isMarkedForDelete())
                                thriftColumns.add(new Column().setName(name));
                            else
                                thriftColumns.add(thriftify(c));
                        }
                    }

                    // Create a new row, add the columns to it, and then add it to the list of rows
                    CqlRow cqlRow = new CqlRow();
                    cqlRow.key = row.key.key;
                    cqlRow.columns = thriftColumns;
                    if (select.isColumnsReversed())
                        Collections.reverse(cqlRow.columns);
                    cqlRows.add(cqlRow);
                    
                    //System.out.println(row.key.getToken());
                    //System.out.println(decode(cqlRow.key));
                    //System.out.println(cqlRow.columns.size());
                    for(int i = 0; i < cqlRow.columns.size(); i++)
                    {
                    	//System.out.print(decode(cqlRow.columns.get(i).name) + " ");
                    	//System.out.println(decode(cqlRow.columns.get(i).value));
                    }
                }

                result.rows = cqlRows;
                return result;

            case INSERT: // insert uses UpdateStatement
            	UpdateStatement insert = (UpdateStatement)statement.statement;
            	insert.getConsistencyLevel().validateForWrite(keyspace);

                keyspace = insert.keyspace == null ? clientState.getKeyspace() : insert.keyspace;
                // permission is checked in prepareRowMutations()
                List<IMutation> rowMutationsInsert = insert.prepareRowMutations(keyspace, clientState, variables);

                for (IMutation mutation : rowMutationsInsert)
                {
                    validateKey(mutation.key());
                }

                StorageProxy.mutate(rowMutationsInsert, insert.getConsistencyLevel());

                result.type = CqlResultType.VOID;
                return result;
                
            case UPDATE:
            	UpdateStatement update = (UpdateStatement)statement.statement;
                update.getConsistencyLevel().validateForWrite(keyspace);

                keyspace = update.keyspace == null ? clientState.getKeyspace() : update.keyspace;
                // permission is checked in prepareRowMutations()
                List<IMutation> rowMutations = update.prepareRowMutations(keyspace, clientState, variables);
            	
                String temp = update.columnFamily.toString();
            	String queryStringForUpdate = "select * from " + temp
            			+ " "+ oriQueryString.substring(oriQueryString.toLowerCase().indexOf("where"), oriQueryString.toLowerCase().indexOf(";")) + " limit 1000000";
            	SelectStatement selectForUpdate = (SelectStatement) getStatement(queryStringForUpdate).statement;

                final String oldKeyspaceForUpdate = clientState.getRawKeyspace();

                if (selectForUpdate.isSetKeyspace())
                {
                    keyspace = CliUtils.unescapeSQLString(selectForUpdate.getKeyspace());
                    ThriftValidation.validateTable(keyspace);
                }
                else if (oldKeyspaceForUpdate == null)
                    throw new InvalidRequestException("no keyspace has been specified");
                else
                    keyspace = oldKeyspaceForUpdate;

                clientState.hasColumnFamilyAccess(keyspace, selectForUpdate.getColumnFamily(), Permission.SELECT);
                metadata = validateColumnFamily(keyspace, selectForUpdate.getColumnFamily());

                // need to do this in here because we need a CFMD.getKeyName()
                selectForUpdate.extractKeyAliasFromColumns(metadata);

                /*
                for (IMutation mutation : rowMutations)
                {
                    validateKey(mutation.key());
                }
                */
                List<org.apache.cassandra.db.Row> rowsForUpdate;
                
                //clean data?
            	String tempString = oriQueryString.replace(" ", "");
            	if(tempString.toLowerCase().indexOf("set")+3 == tempString.toLowerCase().indexOf("clear"))
            	{
            		//yes
            		//xuhao 2013-5-7
            		char delChar = tempString.charAt(tempString.toLowerCase().indexOf("clear") + 7);
            		
            		if(!update.withoutKey && selectForUpdate.numOfClauseRelations() == 1)
                    	StorageProxy.mutate(rowMutations, update.getConsistencyLevel());
                    else if(selectForUpdate.numOfClauseRelations() - 
                    		selectForUpdate.getColumnRelations().size() == 1)
                    {
                    	//rows = getSliceForSpecializedUpdate(metadata, selectForUpdate, variables, rowMutations, update, clientState);
                    	getSliceForSpecializedClear(metadata, selectForUpdate, variables, rowMutations, update, clientState, delChar);
                    }
            		//no key no equip
                    else {
                    	metadata = validateColumnFamily(keyspace, selectForUpdate.getColumnFamily());
    					rowsForUpdate = multiRangeSliceForClear(metadata, selectForUpdate, variables, rowMutations, update, clientState, delChar);
    				}
            		//getSliceForSpecializedClear(metadata, selectForUpdate, variables, rowMutations, update, clientState);
            		
            		result.type = CqlResultType.VOID;
                    return result;
            	}
                
                if(!update.withoutKey && selectForUpdate.numOfClauseRelations() == 1)
                	StorageProxy.mutate(rowMutations, update.getConsistencyLevel());
                else if(selectForUpdate.numOfClauseRelations() - 
                		selectForUpdate.getColumnRelations().size() == 1)
                {
                	rows = getSliceForSpecializedUpdate(metadata, selectForUpdate, variables, rowMutations, update, clientState);
                }
                else {
                	metadata = validateColumnFamily(keyspace, selectForUpdate.getColumnFamily());
					rowsForUpdate = multiRangeSliceForUpdate(metadata, selectForUpdate, variables, rowMutations, update, clientState);
				}
                
                result.type = CqlResultType.VOID;
                return result;

            case BATCH:
                BatchStatement batch = (BatchStatement) statement.statement;
                batch.getConsistencyLevel().validateForWrite(keyspace);

                if (batch.getTimeToLive() != 0)
                    throw new InvalidRequestException("Global TTL on the BATCH statement is not supported.");

                for (AbstractModification up : batch.getStatements())
                {
                    if (up.isSetConsistencyLevel())
                        throw new InvalidRequestException(
                                "Consistency level must be set on the BATCH, not individual statements");

                    if (batch.isSetTimestamp() && up.isSetTimestamp())
                        throw new InvalidRequestException(
                                "Timestamp must be set either on BATCH or individual statements");
                }

                List<IMutation> mutations = batch.getMutations(keyspace, clientState, variables);
                for (IMutation mutation : mutations)
                {
                    validateKey(mutation.key());
                }

                StorageProxy.mutate(mutations, batch.getConsistencyLevel());

                result.type = CqlResultType.VOID;
                return result;

            case USE:
				clientState.validateLogin();                
				clientState.setKeyspace(CliUtils.unescapeSQLString((String) statement.statement));
                result.type = CqlResultType.VOID;

                return result;

            case TRUNCATE:
                Pair<String, String> columnFamily = (Pair<String, String>)statement.statement;
                keyspace = columnFamily.left == null ? clientState.getKeyspace() : columnFamily.left;

                validateColumnFamily(keyspace, columnFamily.right);
                clientState.hasColumnFamilyAccess(keyspace, columnFamily.right, Permission.MODIFY);

                try
                {
                    StorageProxy.truncateBlocking(keyspace, columnFamily.right);
                }
                catch (TimeoutException e)
                {
                    throw new TruncateException(e);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }

                result.type = CqlResultType.VOID;
                return result;

            case DELETE:
                DeleteStatement delete = (DeleteStatement)statement.statement;

                keyspace = delete.keyspace == null ? clientState.getKeyspace() : delete.keyspace;
                // permission is checked in prepareRowMutations()
                List<IMutation> deletions = delete.prepareRowMutations(keyspace, clientState, variables);
            	
                temp = delete.columnFamily.toString();
            	queryStringForUpdate = "select * from " + temp
            			+ " "+ oriQueryString.substring(oriQueryString.indexOf("where"));
            	SelectStatement selectForDelete = (SelectStatement) getStatement(queryStringForUpdate).statement;

                final String oldKeyspaceForDelete = clientState.getRawKeyspace();

                if (selectForDelete.isSetKeyspace())
                {
                    keyspace = CliUtils.unescapeSQLString(selectForDelete.getKeyspace());
                    ThriftValidation.validateTable(keyspace);
                }
                else if (oldKeyspaceForDelete == null)
                    throw new InvalidRequestException("no keyspace has been specified");
                else
                    keyspace = oldKeyspaceForDelete;

                clientState.hasColumnFamilyAccess(keyspace, selectForDelete.getColumnFamily(), Permission.SELECT);
                metadata = validateColumnFamily(keyspace, selectForDelete.getColumnFamily());

                // need to do this in here because we need a CFMD.getKeyName()
                selectForDelete.extractKeyAliasFromColumns(metadata);
                
                if(!delete.withoutKey && selectForDelete.numOfClauseRelations() == 1)
                	StorageProxy.mutate(deletions, delete.getConsistencyLevel());
                else if(selectForDelete.numOfClauseRelations() - 
                		selectForDelete.getColumnRelations().size() == 1)
                {
                	rows = getSliceForSpecializedDelete(metadata, selectForDelete, variables, deletions, delete, clientState);
                }
                else {
                	metadata = validateColumnFamily(keyspace, selectForDelete.getColumnFamily());
					rowsForUpdate = multiRangeSliceForDelete(metadata, selectForDelete, variables, 
							deletions, delete, clientState);
				}
                
                
                for (IMutation deletion : deletions)
                {
                    validateKey(deletion.key());
                }

                StorageProxy.mutate(deletions, delete.getConsistencyLevel());

                result.type = CqlResultType.VOID;
                return result;

            case CREATE_KEYSPACE:
                CreateKeyspaceStatement create = (CreateKeyspaceStatement)statement.statement;
                create.validate();
                ThriftValidation.validateKeyspaceNotSystem(create.getName());
                clientState.hasAllKeyspacesAccess(Permission.CREATE);

                try
                {
                    KSMetaData ksm = KSMetaData.newKeyspace(create.getName(),
                                                            create.getStrategyClass(),
                                                            create.getStrategyOptions(),
                                                            true);
                    ThriftValidation.validateKeyspaceNotYetExisting(ksm.name);
                    MigrationManager.announceNewKeyspace(ksm);
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;

            case CREATE_COLUMNFAMILY:
                CreateColumnFamilyStatement createCf = (CreateColumnFamilyStatement)statement.statement;
                clientState.hasKeyspaceAccess(keyspace, Permission.CREATE);

                try
                {
                    MigrationManager.announceNewColumnFamily(createCf.getCFMetaData(keyspace, variables));
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.toString());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;

            case CREATE_INDEX:
                CreateIndexStatement createIdx = (CreateIndexStatement)statement.statement;
                clientState.hasColumnFamilyAccess(keyspace, createIdx.getColumnFamily(), Permission.ALTER);
                CFMetaData oldCfm = Schema.instance.getCFMetaData(keyspace, createIdx.getColumnFamily());
                if (oldCfm == null)
                    throw new InvalidRequestException("No such column family: " + createIdx.getColumnFamily());

                boolean columnExists = false;
                ByteBuffer columnName = createIdx.getColumnName().getByteBuffer();
                // mutating oldCfm directly would be bad, but mutating a copy is fine.
                CFMetaData cfm = oldCfm.clone();
                for (ColumnDefinition cd : cfm.getColumn_metadata().values())
                {
                    if (cd.name.equals(columnName))
                    {
                        if (cd.getIndexType() != null)
                            throw new InvalidRequestException("Index already exists");
                        if (logger.isDebugEnabled())
                            logger.debug("Updating column {} definition for index {}", cfm.comparator.getString(columnName), createIdx.getIndexName());
                        cd.setIndexType(IndexType.KEYS, Collections.<String, String>emptyMap());
                        cd.setIndexName(createIdx.getIndexName());
                        columnExists = true;
                        break;
                    }
                }
                if (!columnExists)
                    throw new InvalidRequestException("No column definition found for column " + oldCfm.comparator.getString(columnName));

                try
                {
                    cfm.addDefaultIndexNames();
                    MigrationManager.announceColumnFamilyUpdate(cfm);
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.toString());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;

            case DROP_INDEX:
                DropIndexStatement dropIdx = (DropIndexStatement)statement.statement;
                keyspace = clientState.getKeyspace();
                dropIdx.setKeyspace(keyspace);
                clientState.hasColumnFamilyAccess(keyspace, dropIdx.getColumnFamily(), Permission.ALTER);

                try
                {
                    CFMetaData updatedCF = dropIdx.generateCFMetadataUpdate();
                    MigrationManager.announceColumnFamilyUpdate(updatedCF);
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.toString());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;

            case DROP_KEYSPACE:
                String deleteKeyspace = (String)statement.statement;
                ThriftValidation.validateKeyspaceNotSystem(deleteKeyspace);
                clientState.hasKeyspaceAccess(deleteKeyspace, Permission.DROP);

                try
                {
                    MigrationManager.announceKeyspaceDrop(deleteKeyspace);
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;

            case DROP_COLUMNFAMILY:
                String deleteColumnFamily = (String)statement.statement;
                clientState.hasColumnFamilyAccess(keyspace, deleteColumnFamily, Permission.DROP);

                try
                {
                    MigrationManager.announceColumnFamilyDrop(keyspace, deleteColumnFamily);
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;

            case ALTER_TABLE:
                AlterTableStatement alterTable = (AlterTableStatement) statement.statement;

                validateColumnFamily(keyspace, alterTable.columnFamily);
                clientState.hasColumnFamilyAccess(keyspace, alterTable.columnFamily, Permission.ALTER);

                try
                {
                    MigrationManager.announceColumnFamilyUpdate(alterTable.getCFMetaData(keyspace));
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;
        }
        return null;    // We should never get here.
    }

    public static CqlResult process(String queryString, ThriftClientState clientState)
    throws RequestValidationException, RequestExecutionException
    {
    	oriQueryString = queryString;
        logger.trace("CQL QUERY: {}", queryString);
        return processStatement(getStatement(queryString), clientState, new ArrayList<ByteBuffer>(0));
    }

    public static CqlPreparedResult prepare(String queryString, ThriftClientState clientState)
    throws InvalidRequestException, SyntaxException 
    {
        logger.trace("CQL QUERY: {}", queryString);

        CQLStatement statement = getStatement(queryString);
        int statementId = makeStatementId(queryString);
        logger.trace("Discovered "+ statement.boundTerms + " bound variables.");

        clientState.getPrepared().put(statementId, statement);
        logger.trace(String.format("Stored prepared statement #%d with %d bind markers",
                                   statementId,
                                   statement.boundTerms));

        return new CqlPreparedResult(statementId, statement.boundTerms);
    }

    public static CqlResult processPrepared(CQLStatement statement, ThriftClientState clientState, List<ByteBuffer> variables)
    throws RequestValidationException, RequestExecutionException
    {
        // Check to see if there are any bound variables to verify
        if (!(variables.isEmpty() && (statement.boundTerms == 0)))
        {
            if (variables.size() != statement.boundTerms)
                throw new InvalidRequestException(String.format("there were %d markers(?) in CQL but %d bound variables",
                                                                statement.boundTerms,
                                                                variables.size()));

            // at this point there is a match in count between markers and variables that is non-zero

            if (logger.isTraceEnabled())
                for (int i = 0; i < variables.size(); i++)
                    logger.trace("[{}] '{}'", i+1, variables.get(i));
        }

        return processStatement(statement, clientState, variables);
    }

    private static final int makeStatementId(String cql)
    {
        // use the hash of the string till something better is provided
        return cql.hashCode();
    }

    private static Column thriftify(IColumn c)
    {
        ByteBuffer value = (c instanceof CounterColumn)
                           ? ByteBufferUtil.bytes(CounterContext.instance().total(c.value()))
                           : c.value();
        return new Column(c.name()).setValue(value).setTimestamp(c.timestamp());
    }

    private static String getKeyString(CFMetaData metadata)
    {
        String keyString;
        try
        {
            keyString = ByteBufferUtil.string(metadata.getKeyName());
        }
        catch (CharacterCodingException e)
        {
            throw new AssertionError(e);
        }
        return keyString;
    }

    private static CQLStatement getStatement(String queryStr) throws SyntaxException
    {
        try
        {
            // Lexer and parser
            CharStream stream = new ANTLRStringStream(queryStr);
            CqlLexer lexer = new CqlLexer(stream);
            TokenStream tokenStream = new CommonTokenStream(lexer);
            CqlParser parser = new CqlParser(tokenStream);

            // Parse the query string to a statement instance
            CQLStatement statement = parser.query();

            // The lexer and parser queue up any errors they may have encountered
            // along the way, if necessary, we turn them into exceptions here.
            lexer.throwLastRecognitionError();
            parser.throwLastRecognitionError();

            return statement;
        }
        catch (RuntimeException re)
        {
            SyntaxException ire = new SyntaxException("Failed parsing statement: [" + queryStr + "] reason: " + re.getClass().getSimpleName() + " " + re.getMessage());
            throw ire;
        }
        catch (RecognitionException e)
        {
            SyntaxException ire = new SyntaxException("Invalid or malformed CQL query string: " + e.getMessage());
            throw ire;
        }
    }

    private static void validateCountOperation(SelectStatement select) throws InvalidRequestException
    {
        if (select.isWildcard())
            return; // valid count(*)

        if (!select.isColumnRange())
        {
            List<Term> columnNames = select.getColumnNames();
            String firstColumn = columnNames.get(0).getText();

            if (columnNames.size() == 1 && (firstColumn.equals("*") || firstColumn.equals("1")))
                return; // valid count(*) || count(1)
        }

        throw new InvalidRequestException("Only COUNT(*) and COUNT(1) operations are currently supported.");
    }

    private static String bufferToString(ByteBuffer string)
    {
        try
        {
            return ByteBufferUtil.string(string);
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
