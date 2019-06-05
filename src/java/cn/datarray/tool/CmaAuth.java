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

import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.ThriftClientState;
import org.apache.cassandra.thrift.ThriftSessionManager;

public class CmaAuth {
    public static final String AUTH_KS = "micapsdataserver";
    public static final String[] skip_CFs = {"treeview", "latestdatatime", "level", "\"AMDAR\"", "\"WIND_PROFILER\"", "WIND_PROFILER", "AMDAR"};


    public static boolean needAuth(String cf) {
        for(String c : skip_CFs) {
            if( c.equals(cf)) return false;
        }
        return true;
    }
    public static boolean tooLarge(SlicePredicate predicate) {
        return false;
//        if (predicate.slice_range.count <= 50) {
//            return false;
//        }
//       /* String start = ReadWriteLogger.getString(predicate.slice_range.start);
//        String end = ReadWriteLogger.getString(predicate.slice_range.finish);
//        if (start.length()>8 && end.length() >8) {
//            start = start.substring(0,8);
//            end = end.substring(0,8);
//            return !lessThanOneDay(start, end);
//        }
//        return false;
//        */
//        ThriftClientState state = ThriftSessionManager.instance.currentSession();
//        if (state.getRemoteAddress() !=null && state.getUser() != null) {
//            ReadWriteLogger.logLargeLimit(state.getUser().getName(), state.getRemoteAddress().getHostString(), predicate.slice_range.count);
//        } else {
//            ReadWriteLogger.logLargeLimit("unknown",  state.getRemoteAddress().getHostString(), predicate.slice_range.count);
//        }
//       return true;
    }

    private static boolean lessThanOneDay(int start, int end) {
        if (end - start <2){
            // if just one day, or corss a day
            return true;
        }else if (end/10000 - start/10000 >2) {//because GeSihan's method, we need to add start with 10000000
            //cross two years
            return false;
        }else if (end/10000 - start/10000 == 2 && end % 10000 == 101) { //because GeSihan's method, we need to add start with 10000000
            // cross a year
            return true;
        }else {
            //crose a month
            start = start %10000;
            end = end %10000;
            if (start == 131 && end == 201
                || start == 228 && end == 301 || start == 229 && end == 301
                || start == 331 && end == 401
                || start == 430 && end == 501
                || start == 531 && end == 601
                || start == 630 && end == 701
                || start == 731 && end == 801
                || start == 831 && end == 901
                || start == 930 && end == 1001
                || start == 1031 && end == 1101
                || start == 1130 && end == 1201
            ){
                return true;
            }
        }
        return false;

    }
    private static boolean lessThanOneDay(String left, String right) {
        if (left.equals(right)) {
            // if the same day
            return true;
        }

        try
        {
            int start = Integer.valueOf(left);
            int end = Integer.valueOf(right);
            return start < end? lessThanOneDay(start, end) : lessThanOneDay(end, start);
        } catch (NumberFormatException e){
            // if they are not yyyyMMdd
            return true;
        }

    }
}
