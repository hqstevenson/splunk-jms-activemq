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

import javax.jms.ConnectionFactory;

import com.pronoia.splunk.jms.SplunkJmsMessageConsumer;
import com.pronoia.splunk.jms.activemq.internal.ActiveMqBrokerUtils;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;


/**
 * Splunk JMS Message consumer specifically for ActiveMQ.
 */
public class SplunkActiveMqMessageConsumer extends SplunkJmsMessageConsumer {
    String brokerURL;
    String userName;
    String password;

    boolean useRedelivery = true;

    Boolean useExponentialBackOff = true;
    Double backoffMultiplier = 2.0;

    Long initialRedeliveryDelay = 1000L;
    Long maximumRedeliveryDelay = 60000L;
    Integer maximumRedeliveries = -1;

    public SplunkActiveMqMessageConsumer(String destinationName) {
        super(destinationName);
    }

    public SplunkActiveMqMessageConsumer(String destinationName, boolean useTopic) {
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

    @Override
    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        if (useRedelivery && connectionFactory instanceof ActiveMQConnectionFactory) {
            ActiveMQConnectionFactory activeMQConnectionFactory = (ActiveMQConnectionFactory) connectionFactory;
            RedeliveryPolicy redeliveryPolicy = activeMQConnectionFactory.getRedeliveryPolicy();
            if (redeliveryPolicy == null) {
                redeliveryPolicy = new RedeliveryPolicy();
                activeMQConnectionFactory.setRedeliveryPolicy(redeliveryPolicy);
            }
            if (hasInitialRedeliveryDelay()) {
                redeliveryPolicy.setUseExponentialBackOff(useExponentialBackOff);
            }

            if (hasBackoffMultiplier()) {
                redeliveryPolicy.setBackOffMultiplier(backoffMultiplier);
            }

            if (hasInitialRedeliveryDelay()) {
                redeliveryPolicy.setInitialRedeliveryDelay(initialRedeliveryDelay);
            }

            if (hasMaximumRedeliveryDelay()) {
                redeliveryPolicy.setMaximumRedeliveryDelay(maximumRedeliveryDelay);
            }

            if (hasMaximumRedeliveries()) {
                redeliveryPolicy.setMaximumRedeliveries(maximumRedeliveries);
            }
        }
        super.setConnectionFactory(connectionFactory);
    }

    @Override
    public void verifyConfiguration() {
        if (!hasBrokerURL()) {
            throw new IllegalStateException("ActiveMQ Broker URL must be specified");
        }

        super.verifyConfiguration();
    }

    @Override
    public void start() {
        createConnectionFactory();

        super.start();
    }

    protected void createConnectionFactory() {
        if (!hasConnectionFactory()) {
            this.setConnectionFactory(ActiveMqBrokerUtils.createConnectionFactory(log, brokerURL, userName, password));
        }
    }

}
