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

import org.apache.cassandra.net.MessagingService.Verb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 该类是verb的执行线程类。从MessageService中获取某个verb的handler，然后调用其doVerb函数<br>
 * 如果verb可以抛弃并已经超时，那么给对应的drop计数器+1.
 * 否则调用对应的handler处理该verb。
 * @author hxd
 *
 */
public class MessageDeliveryTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(MessageDeliveryTask.class);

    private final MessageIn message;
    private final long constructionTime;
    private final String id;
    /**
     * 该类是verb的执行线程类。
     * 如果verb可以抛弃并已经超时，那么给对应的drop计数器+1.
     * 否则调用对应的handler处理该verb。
     * @author hxd
     *
     */
    public MessageDeliveryTask(MessageIn message, String id, long timestamp)
    {
        assert message != null;
        this.message = message;
        this.id = id;
        constructionTime = timestamp;
    }

    @SuppressWarnings({ "unchecked"})
	public void run()
    {
        MessagingService.Verb verb = message.verb;
        //
        if (MessagingService.DROPPABLE_VERBS.contains(verb)
            && System.currentTimeMillis() > constructionTime + message.getTimeout())//如果可扔弃并且已经超时 就在drop计数器+1
        {
            MessagingService.instance().incrementDroppedMessages(verb);
            return;
        }

        IVerbHandler verbHandler = MessagingService.instance().getVerbHandler(verb);
      
        if (verbHandler == null)
        {
            logger.debug("Unknown verb {}", verb);
            return;		
        }
        if(verb!=Verb.GOSSIP_DIGEST_ACK &&verb!=Verb.GOSSIP_DIGEST_ACK2&& verb!=Verb.GOSSIP_DIGEST_SYN){
        	//logger.debug("----------verbHandler type："+verbHandler.getClass().toString());
        	//logger.debug("----------verbHandler："+verbHandler.toString());
        }
        verbHandler.doVerb(message, id);
    }
}
