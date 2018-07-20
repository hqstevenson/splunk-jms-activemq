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

import java.util.Set;

import javax.jms.Message;

import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServer;
import javax.management.MBeanServerDelegate;
import javax.management.MBeanServerNotification;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import javax.management.relation.MBeanServerNotificationFilter;


import com.pronoia.splunk.eventcollector.EventBuilder;
import com.pronoia.splunk.eventcollector.EventCollectorClient;
import com.pronoia.splunk.eventcollector.SplunkMDCHelper;

import com.pronoia.splunk.jms.eventbuilder.CamelJmsMessageEventBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;


/**
 * Discovers ActiveMQ Brokers and queues running in the same JVM (embedded), and creates SplunkJmsMessageLister instances for the discovered queues.
 *
 * By default, SplunkJmsMessageListeners will be setup to consume messages from JMS Queues with names starting with 'audit.' from all detected brokers.
 *
 * Brokers/queues are detected via JMX Notifications from the MBeanServerDelegate.
 */
public abstract class SplunkEmbeddedActiveMQJmxListenerSupport implements NotificationListener {
    static final String DEFAULT_BROKER_NAME = "*";
    static final String DEFAULT_DESTINATION_TYPE = "Queue";
    static final String DEFAULT_DESTINATION_NAME_PATTERN = "audit.*";

    Logger log = LoggerFactory.getLogger(this.getClass());

    String brokerName = DEFAULT_BROKER_NAME;
    String userName;
    String password;

    boolean useRedelivery = true;

    Boolean useExponentialBackOff = true;
    Double backoffMultiplier = 2.0;

    Long initialRedeliveryDelay = 1000L;
    Long maximumRedeliveryDelay = 60000L;
    Integer maximumRedeliveries = -1;

    String destinationType = DEFAULT_DESTINATION_TYPE;
    String destinationNamePattern = DEFAULT_DESTINATION_NAME_PATTERN;

    EventCollectorClient splunkClient;
    EventBuilder<Message> splunkEventBuilder;

    boolean listenerStarted;

    protected abstract void scheduleConsumerStartup(ObjectName mbeanName);

    public boolean hasBrokerName() {
        return brokerName != null && !brokerName.isEmpty();
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public boolean hasUserName() {
        return userName != null && !userName.isEmpty();
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public boolean hasPassword() {
        return password != null && !password.isEmpty();
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isUseRedelivery() {
        return useRedelivery;
    }

    public void setUseRedelivery(boolean useRedelivery) {
        this.useRedelivery = useRedelivery;
    }

    public boolean hasUseExponentialBackOff() {
        return useExponentialBackOff != null;
    }

    public Boolean getUseExponentialBackOff() {
        return useExponentialBackOff;
    }

    public void setUseExponentialBackOff(Boolean useExponentialBackOff) {
        this.useExponentialBackOff = useExponentialBackOff;
    }

    public boolean hasBackoffMultiplier() {
        return backoffMultiplier != null && backoffMultiplier > 1;
    }

    public Double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public void setBackoffMultiplier(Double backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
    }

    public boolean hasInitialRedeliveryDelay() {
        return initialRedeliveryDelay != null && initialRedeliveryDelay > 0;
    }

    public Long getInitialRedeliveryDelay() {
        return initialRedeliveryDelay;
    }

    public void setInitialRedeliveryDelay(Long initialRedeliveryDelay) {
        this.initialRedeliveryDelay = initialRedeliveryDelay;
    }

    public boolean hasMaximumRedeliveryDelay() {
        return maximumRedeliveryDelay != null && maximumRedeliveryDelay > 0;
    }

    public Long getMaximumRedeliveryDelay() {
        return maximumRedeliveryDelay;
    }

    public void setMaximumRedeliveryDelay(Long maximumRedeliveryDelay) {
        this.maximumRedeliveryDelay = maximumRedeliveryDelay;
    }

    public boolean hasMaximumRedeliveries() {
        return maximumRedeliveries != null;
    }

    public Integer getMaximumRedeliveries() {
        return maximumRedeliveries;
    }

    public void setMaximumRedeliveries(Integer maximumRedeliveries) {
        this.maximumRedeliveries = maximumRedeliveries;
    }

    public boolean hasDestinationType() {
        return destinationType != null && !destinationType.isEmpty();
    }

    public String getDestinationType() {
        return destinationType;
    }

    public void setDestinationType(String destinationType) {
        this.destinationType = destinationType;
    }

    public boolean hasDestinationName() {
        return destinationNamePattern != null && !destinationNamePattern.isEmpty();
    }

    public String getDestinationNamePattern() {
        return destinationNamePattern;
    }

    public void setDestinationNamePattern(String destinationNamePattern) {
        this.destinationNamePattern = destinationNamePattern;
    }

    public boolean hasSplunkClient() {
        return splunkClient != null;
    }

    public EventCollectorClient getSplunkClient() {
        return splunkClient;
    }

    public void setSplunkClient(EventCollectorClient splunkClient) {
        this.splunkClient = splunkClient;
    }

    public boolean hasSplunkEventBuilder() {
        return splunkEventBuilder != null;
    }

    public EventBuilder<Message> getSplunkEventBuilder() {
        return splunkEventBuilder;
    }

    public void setSplunkEventBuilder(EventBuilder<Message> splunkEventBuilder) {
        this.splunkEventBuilder = splunkEventBuilder;
    }

    public boolean isListenerStarted() {
        return listenerStarted;
    }

    void verifyConfiguration() throws IllegalStateException {
        if (!hasSplunkClient()) {
            throw new IllegalStateException("Splunk Client must be specified");
        }

        if (!hasSplunkEventBuilder()) {
            splunkEventBuilder = new CamelJmsMessageEventBuilder();
            log.warn("Splunk EventBuilder<{}> is not specified - using default '{}'", Message.class.getName(), splunkEventBuilder.getClass().getName());
        }

        try (SplunkMDCHelper helper = createMdcHelper()) {
            if (!hasBrokerName()) {
                brokerName = DEFAULT_BROKER_NAME;
                log.warn("ActiveMQ Broker Name is not specified - using default '{}'", brokerName);
            }

            if (!hasDestinationType()) {
                destinationType = DEFAULT_DESTINATION_TYPE;
                log.warn("Destination Type is not specified - using default '{}'", destinationType);
            }

            if (!hasDestinationName()) {
                destinationNamePattern = DEFAULT_DESTINATION_NAME_PATTERN;
                log.warn("Destination Name is not specified - using default '{}'", destinationNamePattern);
            }
        }
    }

    /**
     * Start NotificationListener, watching for the specific queue.
     */
    public synchronized void start() {
        if (isListenerStarted()) {
            try (SplunkMDCHelper helper = createMdcHelper()) {
                log.warn("Attempting to start previously started NotificationListener instance - ignoring");
            }
            return;
        }

        verifyConfiguration();

        try (SplunkMDCHelper helper = createMdcHelper()) {
            final ObjectName destinationObjectNamePattern = createDestinationObjectName();

            log.info("Starting {} with ObjectName pattern {}", this.getClass().getSimpleName(), destinationObjectNamePattern);

            MBeanServerNotificationFilter notificationFilter = new MBeanServerNotificationFilter();
            notificationFilter.enableAllObjectNames();
            notificationFilter.enableType(MBeanServerNotification.REGISTRATION_NOTIFICATION);

            log.debug("Looking for pre-existing destinations");
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            Set<ObjectName> existingDestinationObjectNameSet = mbeanServer.queryNames(createDestinationObjectName(), null);
            if (existingDestinationObjectNameSet != null && !existingDestinationObjectNameSet.isEmpty()) {
                for (ObjectName mbeanName : existingDestinationObjectNameSet) {
                    scheduleConsumerStartup(mbeanName);
                }
            }

            log.debug("Starting JMX NotificationListener watching for ObjectName pattern {}", destinationObjectNamePattern);
            try {
                mbeanServer.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME, this, notificationFilter, null);
            } catch (InstanceNotFoundException instanceNotFoundEx) {
                log.error("Failed to add NotificationListener to '{}' with filter '{}' for '{}' - dynamic destination detection is disabled",
                        MBeanServerDelegate.DELEGATE_NAME.getCanonicalName(),
                        notificationFilter.toString(),
                        destinationObjectNamePattern.getCanonicalName(),
                        instanceNotFoundEx);
            }

            listenerStarted = true;
        }
    }

    /**
     * Stop the NotificationListener.
     */
    public synchronized void stop() {
        try (SplunkMDCHelper helper = createMdcHelper()) {
            log.info("Stopping {} with ObjectName {}", this.getClass().getSimpleName(), createDestinationObjectName());

            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            try {
                listenerStarted = false;
                mbeanServer.removeNotificationListener(MBeanServerDelegate.DELEGATE_NAME, this);
            } catch (InstanceNotFoundException | ListenerNotFoundException removalEx) {
                String errorMessage = String.format("Ignoring exception encountered removed NotificationListener from '%s'", MBeanServerDelegate.DELEGATE_NAME.toString());
                log.error(errorMessage, removalEx);
            }
        }
    }

    @Override
    public synchronized void handleNotification(Notification notification, Object handback) {
        if (notification instanceof MBeanServerNotification) {
            try (SplunkMDCHelper helper = createMdcHelper()) {
                MBeanServerNotification serverNotification = (MBeanServerNotification) notification;
                if (MBeanServerNotification.REGISTRATION_NOTIFICATION.equals(notification.getType())) {
                    final ObjectName destinationObjectNamePattern = createDestinationObjectName();

                    ObjectName mbeanName = serverNotification.getMBeanName();
                    if (destinationObjectNamePattern.apply(mbeanName)) {
                        log.debug("ObjectName '{}' matched '{}' - scheduling MessageListener startup.", mbeanName.getCanonicalName(), destinationObjectNamePattern.getCanonicalName());
                        scheduleConsumerStartup(mbeanName);
                    } else {
                        log.trace("ObjectName '{}' did not match '{}' - ignoring.", mbeanName.getCanonicalName(), destinationObjectNamePattern.getCanonicalName());
                    }
                }
            }
        }
    }

    String createDestinationObjectNameString() {
        String objectNameString = String.format("org.apache.activemq:type=Broker,brokerName=%s,destinationType=%s,destinationName=%s",
                hasBrokerName() ? brokerName : '*',
                hasDestinationType() ? destinationType : '*',
                hasDestinationName() ? destinationNamePattern : '*'
        );

        return objectNameString;
    }

    ObjectName createDestinationObjectName() {
        ObjectName destinationObjectName;

        String objectNameString = createDestinationObjectNameString();

        try {
            destinationObjectName = new ObjectName(objectNameString);
        } catch (MalformedObjectNameException malformedObjectNameEx) {
            String errorMessage = String.format("ObjectName '%s' is malformed", objectNameString);
            throw new IllegalArgumentException(errorMessage, malformedObjectNameEx);
        }

        return destinationObjectName;
    }

    protected SplunkMDCHelper createMdcHelper() {
        return new EmbeddedActiveMQJmxNotificationListenerMDCHelper();
    }

    class EmbeddedActiveMQJmxNotificationListenerMDCHelper extends SplunkMDCHelper {
        public static final String MDC_ACTIVEMQ_JMX_NOTIFICATAION_SOURCE_MEAN = "splunk.jmx.notification.source";

        EmbeddedActiveMQJmxNotificationListenerMDCHelper() {
            addEventBuilderValues(splunkEventBuilder);
            saveContextMap();
            MDC.put(MDC_ACTIVEMQ_JMX_NOTIFICATAION_SOURCE_MEAN, createDestinationObjectName().getCanonicalName());
        }
    }

}
