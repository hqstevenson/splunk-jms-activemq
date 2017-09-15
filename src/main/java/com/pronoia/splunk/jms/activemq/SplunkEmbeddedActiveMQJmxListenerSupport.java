/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the License.
 */

package com.pronoia.splunk.jms.activemq;

import com.pronoia.splunk.eventcollector.EventBuilder;
import com.pronoia.splunk.eventcollector.EventCollectorClient;
import com.pronoia.splunk.jms.eventbuilder.JmsMessageEventBuilder;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  String destinationType = DEFAULT_DESTINATION_TYPE;
  String destinationNamePattern = DEFAULT_DESTINATION_NAME_PATTERN;

  EventCollectorClient splunkClient;
  EventBuilder<Message> splunkEventBuilder;

  boolean listenerStarted = false;

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
      splunkEventBuilder = new JmsMessageEventBuilder();
      log.warn("Splunk EventBuilder<{}> is not specified - using default '{}'", Message.class.getName(), splunkEventBuilder.getClass().getName());
    }

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

  /**
   * Start NotificationListener, watching for the specific queue.
   */
  synchronized public void start() {
    if (isListenerStarted()) {
      log.warn("Attempting to start previously started NotificationListener instance - ignoring");
      return;
    }

    verifyConfiguration();

    final ObjectName destinationObjectNamePattern = createDestinationObjectName();

    log.info("Starting {} with ObjectName pattern {}", this.getClass().getSimpleName(), destinationObjectNamePattern);

    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    MBeanServerNotificationFilter notificationFilter = new MBeanServerNotificationFilter();
    notificationFilter.enableAllObjectNames();
    notificationFilter.enableType(MBeanServerNotification.REGISTRATION_NOTIFICATION);

    log.debug("Looking for pre-existing destinations");
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
      String errorMessage = String.format("Failed to add NotificationListener to '%s' with filter '%s' for '%s' - dynamic destination detection is disabled",
          MBeanServerDelegate.DELEGATE_NAME.getCanonicalName(),
          notificationFilter.toString(),
          destinationObjectNamePattern.getCanonicalName()
      );
      log.error(errorMessage, instanceNotFoundEx);
    }

    listenerStarted = true;
  }

  /**
   * Stop the NotificationListener.
   */
  synchronized public void stop() {
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

  @Override
  public void handleNotification(Notification notification, Object handback) {
    if (notification instanceof MBeanServerNotification) {
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

  protected abstract void scheduleConsumerStartup(ObjectName mbeanName);

  /*
  void callScheduleConsumerStartup(ObjectName mbeanName) {
    String destinationName = mbeanName.getKeyProperty("destinationName");
    boolean useTopic = ("Topic".equals(mbeanName.getKeyProperty("destinationType"))) ? true : false;
    scheduleConsumerStartup(destinationName, useTopic);
  }
  */

  ObjectName createDestinationObjectName() {
    ObjectName destinationObjectName;

    String objectNameString = String.format("org.apache.activemq:type=Broker,brokerName=%s,destinationType=%s,destinationName=%s",
        hasBrokerName() ? brokerName : '*',
        hasDestinationType() ? destinationType : '*',
        hasDestinationName() ? destinationNamePattern : '*'
    );

    try {
      destinationObjectName = new ObjectName(objectNameString);
    } catch (MalformedObjectNameException malformedObjectNameEx) {
      String errorMessage = String.format("ObjectName '%s' is malformed", objectNameString);
      throw new IllegalArgumentException(errorMessage, malformedObjectNameEx);
    }

    return destinationObjectName;
  }

}
