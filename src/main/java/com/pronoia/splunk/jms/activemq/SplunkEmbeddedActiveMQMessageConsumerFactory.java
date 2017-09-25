/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import com.pronoia.splunk.eventcollector.EventBuilder;
import com.pronoia.splunk.eventcollector.EventCollectorClient;
import com.pronoia.splunk.jms.SplunkJmsMessageConsumer;
import com.pronoia.splunk.jms.SplunkJmsMessageListener;
import com.pronoia.splunk.jms.activemq.internal.MessageListenerStartupTask;
import com.pronoia.splunk.jms.eventbuilder.JmsMessageEventBuilder;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;

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

  public SplunkJmsMessageConsumer getMessageListener(String canonicalNameString) {
    return consumerMap.get(canonicalNameString);
  }


  /**
   * Stop the NotificationListener.
   */
  synchronized public void stop() {
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


  @Override
  protected synchronized void scheduleConsumerStartup(ObjectName mbeanName) {
    String objectNameString = mbeanName.getCanonicalName();

    if (consumerMap.containsKey(objectNameString)) {
      log.debug("JMS consumer startup already scheduled for {} - ignoring", objectNameString);
      return;
    }

    log.info("JMS consumer startup for {}", objectNameString);

    String destinationName = mbeanName.getKeyProperty("destinationName");
    boolean useTopic = ("Topic".equals(mbeanName.getKeyProperty("destinationType"))) ? true : false;

    SplunkActiveMqMessageConsumer newMessageConsumer = new SplunkActiveMqMessageConsumer(destinationName, useTopic);

    newMessageConsumer.setReceiveTimeoutMillis(receiveTimeoutMillis);
    newMessageConsumer.setInitialDelaySeconds(initialDelaySeconds);
    newMessageConsumer.setDelaySeconds(delaySeconds);

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

    return;
  }

}
