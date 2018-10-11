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

import javax.jms.ConnectionFactory;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;


/**
 * Splunk JMS Message consumer specifically for ActiveMQ.
 */
public class SplunkActiveMqConsumerRunnable extends AbstractSplunkActiveMqConsumerRunnable {
    String brokerURL;

    public SplunkActiveMqConsumerRunnable(String destinationName) {
        super(destinationName);
    }

    public SplunkActiveMqConsumerRunnable(String destinationName, boolean useTopic) {
        super(destinationName, useTopic);
    }



    public boolean hasBrokerURL() {
        return brokerURL != null && !brokerURL.isEmpty();
    }

    public String getBrokerURL() {
        return brokerURL;
    }

    public void setBrokerURL(String brokerURL) {
        this.brokerURL = brokerURL;
    }

    @Override
    public boolean hasConnectionFactory() {
        return hasBrokerURL() ? true : super.hasConnectionFactory();
    }

    @Override
    public void verifyConfiguration() {
        if (!hasBrokerURL()) {
            throw new IllegalStateException("ActiveMQ Broker URL must be specified");
        }

        super.verifyConfiguration();
    }

    @Override
    protected ConnectionFactory createConnectionFactory() {
        ActiveMQConnectionFactory answer = new ActiveMQConnectionFactory(brokerURL);

        answer.setUserName(userName);
        answer.setPassword(password);
        configureRedelivery(answer);

        return answer;
    }

}
