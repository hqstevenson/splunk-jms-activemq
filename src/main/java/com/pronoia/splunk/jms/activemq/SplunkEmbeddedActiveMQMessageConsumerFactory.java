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

import javax.management.ObjectName;

import com.pronoia.splunk.eventcollector.SplunkMDCHelper;

import com.pronoia.splunk.jms.SplunkJmsMessageConsumer;


/**
 * Discovers ActiveMQ Brokers and queues running in the same JVM (embedded), and creates SplunkJmsMessageLister instances for the discovered queues.
 *
 * By default, SplunkJmsMessageListeners will be setup to consume messages from JMS Queues with names starting with 'audit.' from all detected brokers.
 *
 * Brokers/queues are detected via JMX Notifications from the MBeanServerDelegate.
 */
public class SplunkEmbeddedActiveMQMessageConsumerFactory extends SplunkEmbeddedActiveMQJmxListenerSupport {
    long receiveTimeoutMillis = 1000;
    long initialDelaySeconds = 1;
    long delaySeconds = 60;

    Map<String, SplunkJmsMessageConsumer> consumerMap = new ConcurrentHashMap<>();

    @Override
    protected synchronized void scheduleConsumerStartup(ObjectName mbeanName) {
        String objectNameString = mbeanName.getCanonicalName();

        try (SplunkMDCHelper helper = createMdcHelper()) {
            if (consumerMap.containsKey(objectNameString)) {
                log.debug("JMS consumer startup already scheduled for {} - ignoring", objectNameString);
            } else {
                log.info("JMS consumer startup for {}", objectNameString);

                String destinationName = mbeanName.getKeyProperty("destinationName");
                boolean useTopic = ("Topic".equals(mbeanName.getKeyProperty("destinationType"))) ? true : false;

                SplunkActiveMqMessageConsumer newMessageConsumer = new SplunkActiveMqMessageConsumer(destinationName, useTopic);

                newMessageConsumer.setReceiveTimeoutMillis(receiveTimeoutMillis);
                newMessageConsumer.setInitialDelaySeconds(initialDelaySeconds);
                newMessageConsumer.setDelaySeconds(delaySeconds);

                if (isUseRedelivery()) {
                    newMessageConsumer.setUseRedelivery(useRedelivery);
                    newMessageConsumer.setMaximumRedeliveries(maximumRedeliveries);
                    newMessageConsumer.setInitialRedeliveryDelay(initialRedeliveryDelay);
                    newMessageConsumer.setMaximumRedeliveryDelay(maximumRedeliveryDelay);

                    if (hasUseExponentialBackOff()) {
                        newMessageConsumer.setUseExponentialBackOff(useExponentialBackOff);
                        newMessageConsumer.setBackoffMultiplier(backoffMultiplier);
                    }
                }

                newMessageConsumer.setBrokerURL(String.format("vm://%s?create=false", mbeanName.getKeyProperty("brokerName")));

                if (hasUserName()) {
                    newMessageConsumer.setUserName(userName);
                }
                if (hasPassword()) {
                    newMessageConsumer.setPassword(password);
                }

                newMessageConsumer.setSplunkEventBuilder(splunkEventBuilder.duplicate());

                newMessageConsumer.setSplunkClient(splunkClient);

                if (!consumerMap.containsKey(objectNameString)) {
                    log.info("Scheduling MessageListener for {}", objectNameString);
                    consumerMap.put(objectNameString, newMessageConsumer);
                    newMessageConsumer.start();
                }
            }
        }
    }

    /**
     * Stop the NotificationListener.
     */
    public synchronized void stop() {
        try (SplunkMDCHelper helper = createMdcHelper()) {
            super.stop();

            if (consumerMap != null && !consumerMap.isEmpty()) {
                for (Map.Entry<String, SplunkJmsMessageConsumer> consumerEntry : consumerMap.entrySet()) {
                    SplunkJmsMessageConsumer messageConsumer = consumerEntry.getValue();
                    if (messageConsumer != null && messageConsumer.isConnectionStarted()) {
                        log.info("Stopping consumer for {}", consumerEntry.getKey());
                        messageConsumer.stop();
                    }
                }
                consumerMap.clear();
            }
        }
    }

    public SplunkJmsMessageConsumer getMessageListener(String canonicalNameString) {
        return consumerMap.get(canonicalNameString);
    }

    protected SplunkMDCHelper createMdcHelper() {
        return new EmbeddedActiveMQJmxNotificationListenerMDCHelper();
    }

    class EmbeddedActiveMQJmxNotificationListenerMDCHelper extends SplunkMDCHelper {
        public static final String MDC_ACTIVEMQ_JMX_NOTIFICATAION_SOURCE_MEAN = "splunk.jmx.notification.source";

        EmbeddedActiveMQJmxNotificationListenerMDCHelper() {
            addEventBuilderValues(splunkEventBuilder);
        }
    }

}
