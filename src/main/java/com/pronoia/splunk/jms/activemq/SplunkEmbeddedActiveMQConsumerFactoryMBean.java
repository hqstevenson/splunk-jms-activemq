/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pronoia.splunk.jms.activemq;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Discovers ActiveMQ Brokers and queues running in the same JVM (embedded), and creates SplunkJmsMessageLister instances for the discovered queues.
 *
 * By default, SplunkJmsMessageListeners will be setup to consume messages from JMS Queues with names starting with 'audit.' from all detected brokers.
 *
 * Brokers/queues are detected via JMX Notifications from the MBeanServerDelegate.
 */
public interface SplunkEmbeddedActiveMQConsumerFactoryMBean {

    Map<String, SplunkEmbeddedActiveMqConsumerRunnable> consumerMap = new ConcurrentHashMap<>();

    boolean isRunning();

    String getSplunkClientId();

    String getConsumerFactoryId();

    void start();

    void stop();

    void restart();

    Long getReceiveTimeoutMillis();

    Long getInitialDelaySeconds();

    Long getDelaySeconds();

    Set<Integer> getConsumedHttpStatusCodes();

    Set<Integer> getConsumedSplunkStatusCodes();

    Set<String> getConsumerKeys();

    Date getStartTime();

    Date getStopTime();

    Date getLastConsumerRegisteredTime();

    String getLastRegisteredConsumerKey();

    Date getLastConsumerUnregisteredTime();

    String getLastUnregisteredConsumerKey();
}
