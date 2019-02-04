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

import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import com.pronoia.splunk.eventcollector.SplunkMDCHelper;


/**
 * Discovers ActiveMQ Brokers and queues running in the same JVM (embedded), and creates SplunkJmsMessageLister instances for the discovered queues.
 *
 * By default, SplunkJmsMessageListeners will be setup to consume messages from JMS Queues with names starting with 'audit.' from all detected brokers.
 *
 * Brokers/queues are detected via JMX Notifications from the MBeanServerDelegate.
 */
public class SplunkEmbeddedActiveMQConsumerFactory extends SplunkEmbeddedActiveMQJmxListenerSupport implements SplunkEmbeddedActiveMQConsumerFactoryMBean {
    static AtomicInteger factoryCounter = new AtomicInteger(1);
    String consumerFactoryId;
    ObjectName consumerFactoryObjectName;

    Long receiveTimeoutMillis;
    Long initialDelaySeconds;
    Long delaySeconds;

    Date startTime;
    Date stopTime;

    Date lastConsumerRegisteredTime;
    ObjectName lastConsumerRegisteredObjectName;

    Date lastConsumerUnregisteredTime;
    ObjectName lastConsumerUnregisteredObjectName;

    Map<String, SplunkEmbeddedActiveMqConsumerRunnable> consumerMap = new ConcurrentHashMap<>();

    Set<Integer> consumedHttpStatusCodes;
    Set<Integer> consumedSplunkStatusCodes;

    {
        consumedHttpStatusCodes = new HashSet<>();
        consumedHttpStatusCodes.add(200);

        consumedSplunkStatusCodes = new HashSet<>();
        consumedSplunkStatusCodes.add(0); // Success
        consumedSplunkStatusCodes.add(5); // No Data
        consumedSplunkStatusCodes.add(12); // Event field is required
        consumedSplunkStatusCodes.add(13); // Event field cannot be blank
    }

    @Override
    public boolean isRunning() {
        return isListenerStarted();
    }

    @Override
    public Date getStartTime() {
        return startTime;
    }

    @Override
    public Date getStopTime() {
        return stopTime;
    }

    @Override
    public String getConsumerFactoryId() {
        if (consumerFactoryId == null || consumerFactoryId.isEmpty()) {
            consumerFactoryId = String.format("splunk-embedded-activemq-message-consumer-factory-%d", factoryCounter.getAndIncrement());
        }

        return consumerFactoryId;
    }

    public void setConsumerFactoryId(String consumerFactoryId) {
        this.consumerFactoryId = consumerFactoryId;
    }

    @Override
    public String getSplunkClientId() {
        return getSplunkClient().getClientId();
    }

    @Override
    public Date getLastConsumerRegisteredTime() {
        return lastConsumerRegisteredTime;
    }

    @Override
    public String getLastRegisteredConsumerKey() {
        return (lastConsumerRegisteredObjectName != null) ? lastConsumerRegisteredObjectName.getCanonicalName() : null;
    }

    @Override
    public Date getLastConsumerUnregisteredTime() {
        return lastConsumerUnregisteredTime;
    }

    @Override
    public String getLastUnregisteredConsumerKey() {
        return (lastConsumerUnregisteredObjectName != null) ? lastConsumerUnregisteredObjectName.getCanonicalName() : null;
    }

    public void initialize() {
        registerMBean();
        start();
    }

    public void destroy() {
        stop();
        unregisterMBean();
    }

    @Override
    public Set<String> getConsumerKeys() {
        return (consumerMap != null) ? consumerMap.keySet() : new HashSet<>();
    }

    public synchronized boolean isConsumerRegistered(ObjectName destinationObjectName) {
        return consumerMap.containsKey(destinationObjectName.getCanonicalName());
    }

    public synchronized boolean registerConsumer(SplunkEmbeddedActiveMqConsumerRunnable consumerRunnable) {
        String consumerKey = consumerRunnable.getDestinationMBeanObjectName().getCanonicalName();

        SplunkEmbeddedActiveMqConsumerRunnable previouslyRegisteredConsumer =
                consumerMap.putIfAbsent(consumerKey, consumerRunnable);
        if (previouslyRegisteredConsumer != null) {
            log.warn("Failed to register consumer - a consumer is already registered for {}", consumerKey);
            return false;
        } else {
            lastConsumerRegisteredTime = new Date();
            lastConsumerRegisteredObjectName = consumerRunnable.getDestinationMBeanObjectName();
        }

        return true;
    }

    public synchronized boolean unregisterConsumer(SplunkEmbeddedActiveMqConsumerRunnable consumerRunnable) {
        String consumerKey = consumerRunnable.getDestinationMBeanObjectName().getCanonicalName();
        SplunkEmbeddedActiveMqConsumerRunnable removedConsumer = consumerMap.remove(consumerKey);
        if (removedConsumer != null) {
            lastConsumerUnregisteredTime = new Date();
            lastConsumerUnregisteredObjectName = removedConsumer.getDestinationMBeanObjectName();
            removedConsumer.destroy();
            return true;
        }

        log.warn("Failed to unregister consumer - a consumer is not registered for {}", consumerKey);
        return false;
    }

    @Override
    protected synchronized void scheduleConsumerStartup(ObjectName destinationObjectName) {
        String objectNameString = destinationObjectName.getCanonicalName();

        try (SplunkMDCHelper helper = createMdcHelper()) {
            SplunkEmbeddedActiveMqConsumerRunnable newConsumer = new SplunkEmbeddedActiveMqConsumerRunnable(this, destinationObjectName);
            if (registerConsumer(newConsumer)) {
                if (hasReceiveTimeoutMillis()) {
                    newConsumer.setReceiveTimeoutMillis(receiveTimeoutMillis);
                }

                if ( hasInitialDelaySeconds()) {
                    newConsumer.setInitialDelaySeconds(initialDelaySeconds);
                }
                if (hasDelaySeconds()) {
                    newConsumer.setDelaySeconds(delaySeconds);
                }

                if (hasUseRedelivery()) {
                    if (getUseRedelivery()) {
                        newConsumer.setUseRedelivery(true);
                        if (hasMaximumRedeliveries()) {
                            newConsumer.setMaximumRedeliveries(maximumRedeliveries);
                        }

                        if (hasDelaySeconds()) {
                            newConsumer.setDelaySeconds(delaySeconds);
                        }

                        if (hasInitialDelaySeconds()) {
                            newConsumer.setInitialRedeliveryDelay(initialRedeliveryDelay);
                        }

                        if (hasMaximumRedeliveryDelay()) {
                            newConsumer.setMaximumRedeliveryDelay(maximumRedeliveryDelay);
                        }

                        if (hasUseExponentialBackOff()) {
                            newConsumer.setUseExponentialBackOff(useExponentialBackOff);
                        }
                        if (hasBackoffMultiplier()) {
                            newConsumer.setBackoffMultiplier(backoffMultiplier);
                        }
                    } else {
                        newConsumer.setUseRedelivery(false);
                    }
                }

                if (hasUserName()) {
                    newConsumer.setUserName(userName);
                }
                if (hasPassword()) {
                    newConsumer.setPassword(password);
                }

                newConsumer.setSplunkEventBuilder(splunkEventBuilder.duplicate());

                newConsumer.setSplunkClient(splunkClient);

                if (hasConsumedHttpStatusCodes()) {
                    newConsumer.setConsumedHttpStatusCodes(getConsumedHttpStatusCodes());
                }

                if (hasConsumedSplunkStatusCodes()) {
                    newConsumer.setConsumedSplunkStatusCodes(getConsumedSplunkStatusCodes());
                }

                newConsumer.initialize();
            } else {
                log.debug("JMS consumer startup already register for {} - ignoring", objectNameString);
            }
        }
    }

    @Override
    public synchronized void start() {
        super.start();
        startTime = new Date();
    }


    /**
     * Stop the NotificationListener.
     */
    @Override
    public synchronized void stop() {
        try (SplunkMDCHelper helper = createMdcHelper()) {
            super.stop();
            stopTime = new Date();

            if (consumerMap != null && !consumerMap.isEmpty()) {
                for (SplunkEmbeddedActiveMqConsumerRunnable registeredConsumer : consumerMap.values()) {
                    unregisterConsumer(registeredConsumer);
                }
            }
        }
    }

    public boolean hasReceiveTimeoutMillis() {
        return receiveTimeoutMillis != null && receiveTimeoutMillis > 0;
    }

    public Long getReceiveTimeoutMillis() {
        return receiveTimeoutMillis;
    }

    public void setReceiveTimeoutMillis(String receiveTimeoutMillis) {
        try {
            setReceiveTimeoutMillis( (receiveTimeoutMillis != null) ? Long.valueOf(receiveTimeoutMillis) : null);
        } catch (NumberFormatException numberFormatEx) {
            log.warn("Invalid value {} set for receiveTimeoutMillis - using default", receiveTimeoutMillis, numberFormatEx);
        }
    }

    public void setReceiveTimeoutMillis(Long receiveTimeoutMillis) {
        this.receiveTimeoutMillis = receiveTimeoutMillis;
    }

    public boolean hasInitialDelaySeconds() {
        return initialDelaySeconds != null && initialDelaySeconds > 0;
    }

    public Long getInitialDelaySeconds() {
        return initialDelaySeconds;
    }

    public void setInitialDelaySeconds(String initialDelaySeconds) {
        try {
            setInitialDelaySeconds((initialDelaySeconds != null) ? Long.valueOf(initialDelaySeconds) : null);
        } catch (NumberFormatException numberFormatEx) {
            log.warn("Invalid value {} set for initialDelaySeconds - using default", initialDelaySeconds, numberFormatEx);
        }
    }

    public void setInitialDelaySeconds(Long initialDelaySeconds) {
        this.initialDelaySeconds = initialDelaySeconds;
    }

    public boolean hasDelaySeconds() {
        return delaySeconds != null && delaySeconds > 0;
    }

    public Long getDelaySeconds() {
        return delaySeconds;
    }

    public void setDelaySeconds(String delaySeconds) {
        try {
            setDelaySeconds((delaySeconds != null) ? Long.valueOf(delaySeconds) : null);
        } catch (NumberFormatException numberFormatEx) {
            log.warn("Invalid value {} set for delaySeconds - using default", delaySeconds, numberFormatEx);
        }
    }

    public void setDelaySeconds(Long delaySeconds) {
        this.delaySeconds = delaySeconds;
    }

    public boolean hasConsumedHttpStatusCodes() {
        return consumedHttpStatusCodes != null && !consumedHttpStatusCodes.isEmpty();
    }

    @Override
    public Set<Integer> getConsumedHttpStatusCodes() {
        return consumedHttpStatusCodes;
    }

    public void setConsumedHttpStatusCodes(Set<Integer> consumedHttpStatusCodes) {
        if (consumedHttpStatusCodes != null) {
            if (this.consumedHttpStatusCodes == null) {
                this.consumedHttpStatusCodes = new HashSet<>();
            } else {
                this.consumedHttpStatusCodes.clear();
            }
            this.consumedHttpStatusCodes.addAll(consumedHttpStatusCodes);
        }
    }

    public boolean hasConsumedSplunkStatusCodes() {
        return consumedSplunkStatusCodes != null && !consumedSplunkStatusCodes.isEmpty();
    }

    @Override
    public Set<Integer> getConsumedSplunkStatusCodes() {
        return consumedSplunkStatusCodes;
    }

    public void setConsumedSplunkStatusCodes(Set<Integer> consumedSplunkStatusCodes) {
        if (consumedSplunkStatusCodes != null) {
            if (this.consumedSplunkStatusCodes == null) {
                this.consumedSplunkStatusCodes = new HashSet<>();
            } else {
                this.consumedSplunkStatusCodes.clear();
            }
            this.consumedSplunkStatusCodes.addAll(consumedSplunkStatusCodes);
        }
    }

    void registerMBean() {
        String newFactoryObjectNameString = String.format("com.pronoia.splunk.httpec:type=%s,id=%s", this.getClass().getSimpleName(), getConsumerFactoryId());
        try {
            consumerFactoryObjectName = new ObjectName(newFactoryObjectNameString);
        } catch (MalformedObjectNameException malformedNameEx) {
            log.warn("Failed to create ObjectName for string {} - MBean will not be registered", newFactoryObjectNameString, malformedNameEx);
            return;
        }

        try {
            ManagementFactory.getPlatformMBeanServer().registerMBean(this, consumerFactoryObjectName);
        } catch (InstanceAlreadyExistsException allreadyExistsEx) {
            log.warn("MBean already registered for consumer factory {}", consumerFactoryObjectName, allreadyExistsEx);
        } catch (MBeanRegistrationException registrationEx) {
            log.warn("MBean registration failure for consumer factory {}", newFactoryObjectNameString, registrationEx);
        } catch (NotCompliantMBeanException nonCompliantMBeanEx) {
            log.warn("Invalid MBean for consumer factory {}", newFactoryObjectNameString, nonCompliantMBeanEx);
        }

    }

    void unregisterMBean() {
        if (consumerFactoryObjectName != null) {
            try {
                ManagementFactory.getPlatformMBeanServer().unregisterMBean(consumerFactoryObjectName);
            } catch (InstanceNotFoundException | MBeanRegistrationException unregisterEx) {
                log.warn("Failed to unregister consumer factory MBean {}", consumerFactoryObjectName.getCanonicalName(), unregisterEx);
            } finally {
                consumerFactoryObjectName = null;
            }
        }
    }

    protected SplunkMDCHelper createMdcHelper() {
        return new EmbeddedActiveMQJmxNotificationListenerMDCHelper();
    }

    class EmbeddedActiveMQJmxNotificationListenerMDCHelper extends SplunkMDCHelper {
        EmbeddedActiveMQJmxNotificationListenerMDCHelper() {
            addEventBuilderValues(splunkEventBuilder);
        }
    }

}
