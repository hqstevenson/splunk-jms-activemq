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

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.ConnectionFactory;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.jmx.HealthStatus;

/**
 *
 */
public class SplunkEmbeddedActiveMqConsumerRunnable extends AbstractSplunkActiveMqConsumerRunnable implements SplunkEmbeddedActiveMqConsumerRunnableMBean {
    static AtomicInteger embeddedConsumerRunnableCounter = new AtomicInteger(1);
    final SplunkEmbeddedActiveMQConsumerFactory consumerFactory;
    String embeddedConsumerRunnableId;

    final ObjectName destinationMBeanObjectName;
    ObjectName consumerObjectName;

    public SplunkEmbeddedActiveMqConsumerRunnable(SplunkEmbeddedActiveMQConsumerFactory consumerFactory, ObjectName destinationMBeanObjectName) {
        super(destinationMBeanObjectName.getKeyProperty("destinationName"), "Topic".equals(destinationMBeanObjectName.getKeyProperty("destinationType")) );
        this.consumerFactory = consumerFactory;
        this.destinationMBeanObjectName = destinationMBeanObjectName;
    }

    @Override
    public String getFactoryId() {
        return consumerFactory.getConsumerFactoryId();
    }

    public ObjectName getDestinationMBeanObjectName() {
        return destinationMBeanObjectName;
    }

    public String getEmbeddedConsumerRunnableId() {
        if (embeddedConsumerRunnableId == null || embeddedConsumerRunnableId.isEmpty()) {
            embeddedConsumerRunnableId = String.format("%s-runnable-%d", getFactoryId(), embeddedConsumerRunnableCounter.getAndIncrement());
        }
        return embeddedConsumerRunnableId;
    }

    public void setEmbeddedConsumerRunnableId(String embeddedConsumerRunnableId) {
        this.embeddedConsumerRunnableId = embeddedConsumerRunnableId;
    }

    @Override
    public String getBrokerName() {
        return destinationMBeanObjectName.getKeyProperty("brokerName");
    }

    @Override
    public String getDestinationType() {
        return destinationMBeanObjectName.getKeyProperty("destinationType");
    }

    @Override
    public String getDestinationName() {
        return destinationMBeanObjectName.getKeyProperty("destinationName");
    }

    @Override
    public boolean hasConnectionFactory() {
        return true;
    }

    @Override
    protected ConnectionFactory createConnectionFactory() {
        if (super.hasConnectionFactory()) {
            return super.getConnectionFactory();
        }

        ActiveMQConnectionFactory connectionFactory = null;

        if (isBrokerRunning()) {
            connectionFactory = new ActiveMQConnectionFactory(String.format("vm://%s?create=false&waitForStart=60000", getBrokerName()));
            connectionFactory.setUserName(userName);
            connectionFactory.setPassword(password);

            configureRedelivery(connectionFactory);
        }

        return connectionFactory;
    }

    public void initialize() {
        registerMBean();
        start();
    }

    public void destroy() {
        stop();
        unregisterMBean();
        consumerFactory.unregisterConsumer(this);
    }

    public boolean isBrokerRunning() {
        String brokerHealthServiceObjectNameString = String.format("org.apache.activemq:type=Broker,brokerName=%s,service=Health", getBrokerName());
        ObjectName brokerHealthServiceObjectName;

        try {
            brokerHealthServiceObjectName = new ObjectName(brokerHealthServiceObjectNameString);
        } catch (MalformedObjectNameException malformedObjectNameEx) {
            log.warn("ObjectName {} is malformed", brokerHealthServiceObjectNameString, malformedObjectNameEx);
            return false;
        }

        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

        Set<ObjectName> objectNames = mbeanServer.queryNames(brokerHealthServiceObjectName, null);
        if (objectNames != null && !objectNames.isEmpty()) {
            for (ObjectName brokerHealthObjectName : objectNames) {
                Object healthListObject = null;
                try {
                    healthListObject = mbeanServer.invoke(brokerHealthObjectName, "healthList", null, null);
                    if (healthListObject != null) {
                        if (healthListObject instanceof List) {
                            List<HealthStatus> healthStatusList = (List<HealthStatus>) healthListObject;
                            for (HealthStatus healthStatus : healthStatusList) {
                                String healthLevel = healthStatus.getLevel();
                                switch (healthLevel) {
                                    case "WARNING":
                                        log.debug("Health Status: {}", healthStatus);
                                        break;
                                    default:
                                        log.warn("Unknown Health Status: {}", healthStatus);
                                        break;
                                }
                            }
                        } else {
                            log.warn("Returned object from healthList invocation is not a List: {}", healthListObject.getClass().getName());
                        }
                    }
                } catch (InstanceNotFoundException | MBeanException | ReflectionException jmxInvokeEx) {
                    log.warn("Exception encountered invoking healthList method", jmxInvokeEx);
                    return false;
                }
            }
            return true;
        }

        log.warn("Health Service not found for {}", brokerHealthServiceObjectNameString);
        return false;
    }

    void registerMBean() {
        String newConsumerObjectNameString = String.format("com.pronoia.splunk.httpec:type=%s,id=%s",
                this.getClass().getSimpleName(), getEmbeddedConsumerRunnableId());
        try {
            consumerObjectName = new ObjectName(newConsumerObjectNameString);
        } catch (MalformedObjectNameException malformedNameEx) {
            log.warn("Failed to create ObjectName for string {} - MBean will not be registered", newConsumerObjectNameString, malformedNameEx);
            return;
        }

        try {
            ManagementFactory.getPlatformMBeanServer().registerMBean(this, consumerObjectName);
        } catch (InstanceAlreadyExistsException allreadyExistsEx) {
            log.warn("MBean already registered for name {}", consumerObjectName, allreadyExistsEx);
        } catch (MBeanRegistrationException registrationEx) {
            log.warn("MBean registration failure for name {}", consumerObjectName, registrationEx);
        } catch (NotCompliantMBeanException nonCompliantMBeanEx) {
            log.warn("Invalid MBean for name {}", consumerObjectName, nonCompliantMBeanEx);
        }

    }

    void unregisterMBean() {
        if (consumerObjectName != null) {
            try {
                ManagementFactory.getPlatformMBeanServer().unregisterMBean(consumerObjectName);
            } catch (InstanceNotFoundException | MBeanRegistrationException unregisterEx) {
                log.warn("Failed to unregister consumer MBean {}", consumerObjectName.getCanonicalName(), unregisterEx);
            } finally {
                consumerObjectName = null;
            }
        }
    }
}
