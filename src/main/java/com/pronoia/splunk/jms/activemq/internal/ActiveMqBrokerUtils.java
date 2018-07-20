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
package com.pronoia.splunk.jms.activemq.internal;

import java.lang.management.ManagementFactory;

import java.util.Set;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;

import org.slf4j.Logger;


/**
 * Utility class for ActiveMQ.
 */
public final class ActiveMqBrokerUtils {
    public static final String BROKER_OBJECT_NAME_PATTERN_STRING = "org.apache.activemq:type=Broker,brokerName=*";

    private ActiveMqBrokerUtils() {
        // Unused - utility class
    }

    /**
     * Find the name of the running ActiveMQ broker using JMX.
     *
     * @param log the Logger instance for logging warnings.
     *
     * @return the name of the ActiveMQ Broker
     */
    public static String findEmbeddedBrokerName(Logger log) {
        String answer = null;

        ObjectName brokerObjectNamePattern = null;

        try {
            brokerObjectNamePattern = new ObjectName(BROKER_OBJECT_NAME_PATTERN_STRING);
        } catch (MalformedObjectNameException malformedObjectNameEx) {
            String errorMessage = String.format("ObjectName '%s' is malformed", BROKER_OBJECT_NAME_PATTERN_STRING);
            throw new IllegalArgumentException(errorMessage, malformedObjectNameEx);
        }

        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

        Set<ObjectName> objectNames = mbeanServer.queryNames(brokerObjectNamePattern, null);
        if (objectNames != null && !objectNames.isEmpty()) {
            for (ObjectName brokerName : objectNames) {
                answer = brokerName.getKeyProperty("brokerName");
                break;
            }
            if (objectNames.size() > 1) {
                log.warn("Multiple brokers were found - using first one {}", answer);
            }
        } else {
            log.warn("No embedded brokers found");
        }

        return answer;
    }

    /**
     * Create a VM-Transport URL to connect to an embedded broker.
     *
     * @param brokerName the name of the broker1
     *
     * @return the VM-Transport URL as a String
     */
    public static String createEmbeddedBrokerURL(String brokerName) {
        if (brokerName != null && !brokerName.isEmpty()) {
            return String.format("vm://%s?create=false", brokerName);
        }

        return brokerName;
    }

    /**
     * Find the VM-Transport URL for connecting to the running ActiveMQ broker using JMX.
     *
     * @param log the Logger instance for logging warnings.
     *
     * @return the VM-Transport URL as a String
     */
    public static String findEmbeddedBrokerURL(Logger log) {
        return createEmbeddedBrokerURL(findEmbeddedBrokerName(log));
    }


    /**
     * Create an ActiveMQ Connection Factory.
     *
     * @param log the Logger instance for logging warnings.
     * @param brokerURL the Broker URL
     * @param userName the broker username (ignored if null or empty)
     * @param password the password for the broker user (ignored if null or empty or if the username is null or empty)
     *
     * @return an ActvieMQ Connection Factory
     */
    public static ActiveMQConnectionFactory createConnectionFactory(Logger log, String brokerURL, String userName, String password) {
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();

        activeMQConnectionFactory.setBrokerURL(brokerURL);

        if (userName != null && !userName.isEmpty()) {
            activeMQConnectionFactory.setUserName(userName);
            if (password != null && !password.isEmpty()) {
                activeMQConnectionFactory.setPassword(password);
            } else {
                log.warn("ActiveMQ password is not specified");
            }
        } else {
            log.warn("ActiveMQ user name is not specified");
        }

        return activeMQConnectionFactory;
    }

}
