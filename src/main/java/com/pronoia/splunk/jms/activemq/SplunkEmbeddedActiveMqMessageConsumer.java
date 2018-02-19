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

import com.pronoia.splunk.jms.SplunkJmsMessageConsumer;
import com.pronoia.splunk.jms.activemq.internal.ActiveMqBrokerUtils;

/**
 *
 */
public class SplunkEmbeddedActiveMqMessageConsumer extends SplunkJmsMessageConsumer {
    String brokerName;
    String userName;
    String password;

    public SplunkEmbeddedActiveMqMessageConsumer() {
    }

    public SplunkEmbeddedActiveMqMessageConsumer(String destinationName) {
        super(destinationName);
    }

    public SplunkEmbeddedActiveMqMessageConsumer(String destinationName, boolean useTopic) {
        super(destinationName, useTopic);
    }

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

    @Override
    public void verifyConfiguration() {
        super.verifyConfiguration();
    }

    @Override
    public void start() {
        createConnectionFactory();

        super.start();
    }

    protected void createConnectionFactory() {
        if (!hasConnectionFactory()) {
            if (!hasBrokerName()) {
                this.brokerName = ActiveMqBrokerUtils.findEmbeddedBrokerName(log);
            }

            this.setConnectionFactory(ActiveMqBrokerUtils.createConnectionFactory(log, ActiveMqBrokerUtils.createEmbeddedBrokerURL(brokerName), userName, password));
        }
    }
}
