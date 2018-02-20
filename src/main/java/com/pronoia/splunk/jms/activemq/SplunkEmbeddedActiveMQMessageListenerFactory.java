/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pronoia.splunk.jms.activemq;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import com.pronoia.splunk.jms.SplunkJmsMessageListener;
import com.pronoia.splunk.jms.activemq.internal.MessageListenerStartupTask;
import com.pronoia.splunk.jms.activemq.internal.NamedThreadFactory;

/**
 * Discovers ActiveMQ Brokers and queues running in the same JVM (embedded), and creates SplunkJmsMessageLister instances for the discovered queues.
 *
 * By default, SplunkJmsMessageListeners will be setup to consume messages from JMS Queues with names starting with 'audit.' from all detected brokers.
 *
 * Brokers/queues are detected via JMX Notifications from the MBeanServerDelegate.
 */
public class SplunkEmbeddedActiveMQMessageListenerFactory extends SplunkEmbeddedActiveMQJmxListenerSupport {
    long startupDelay = 15;
    TimeUnit startupDelayUnit = TimeUnit.SECONDS;

    ScheduledExecutorService startupExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getSimpleName()));

    Map<String, SplunkJmsMessageListener> listenerMap = new ConcurrentHashMap<>();

    @Override
    protected synchronized void scheduleConsumerStartup(ObjectName mbeanName) {
        String objectNameString = mbeanName.getCanonicalName();

        if (listenerMap.containsKey(objectNameString)) {
            log.debug("MessageListener startup already scheduled for {} - ignoring", objectNameString);
        } else {

            log.info("Scheduling MessageListener startup for {}", objectNameString);

            String destinationName = mbeanName.getKeyProperty("destinationName");
            boolean useTopic = ("Topic".equals(mbeanName.getKeyProperty("destinationType"))) ? true : false;

            SplunkActiveMqMessageListener newMessageListener = new SplunkActiveMqMessageListener(destinationName, useTopic);

            newMessageListener.setBrokerURL(String.format("vm://%s?create=false", mbeanName.getKeyProperty("brokerName")));

            if (hasUserName()) {
                newMessageListener.setUserName(userName);
            }
            if (hasPassword()) {
                newMessageListener.setPassword(password);
            }

            if (isUseRedelivery()) {
                newMessageListener.setUseRedelivery(useRedelivery);
                newMessageListener.setMaximumRedeliveries(maximumRedeliveries);
                newMessageListener.setInitialRedeliveryDelay(initialRedeliveryDelay);
                newMessageListener.setMaximumRedeliveryDelay(maximumRedeliveryDelay);

                if (hasUseExponentialBackOff()) {
                    newMessageListener.setUseExponentialBackOff(useExponentialBackOff);
                    newMessageListener.setBackoffMultiplier(backoffMultiplier);
                }
            }

            newMessageListener.setSplunkEventBuilder(splunkEventBuilder.duplicate());

            newMessageListener.setSplunkClient(splunkClient);

            if (!listenerMap.containsKey(objectNameString)) {
                log.info("Scheduling MessageListener for {}", objectNameString);
                listenerMap.put(objectNameString, newMessageListener);
                startupExecutor.schedule(new MessageListenerStartupTask(this, objectNameString), startupDelay, startupDelayUnit);
            }
        }
    }

    /**
     * Stop the NotificationListener.
     */
    public synchronized void stop() {
        super.stop();

        if (listenerMap != null && !listenerMap.isEmpty()) {
            for (Map.Entry<String, SplunkJmsMessageListener> listenerEntry : listenerMap.entrySet()) {
                SplunkJmsMessageListener messageListener = listenerEntry.getValue();
                if (messageListener != null && messageListener.isConnectionStarted()) {
                    log.info("Stopping listener for {}", listenerEntry.getKey());
                    messageListener.stop();
                }
            }
            listenerMap.clear();
        }
    }

    public long getStartupDelay() {
        return startupDelay;
    }

    public void setStartupDelay(long startupDelay) {
        this.startupDelay = startupDelay;
    }

    public TimeUnit getStartupDelayUnit() {
        return startupDelayUnit;
    }

    public void setStartupDelayUnit(TimeUnit startupDelayUnit) {
        this.startupDelayUnit = startupDelayUnit;
    }

    public SplunkJmsMessageListener getMessageListener(String canonicalNameString) {
        return listenerMap.get(canonicalNameString);
    }

}
