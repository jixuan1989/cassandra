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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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


}
