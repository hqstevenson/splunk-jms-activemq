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

import com.pronoia.splunk.jms.SplunkJmsConsumerRunnable;

import javax.jms.ConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;


/**
 * Splunk JMS Message consumer specifically for ActiveMQ.
 */
public abstract class AbstractSplunkActiveMqConsumerRunnable extends SplunkJmsConsumerRunnable {
    String userName;
    String password;

    boolean useRedelivery = true;

    long redeliveryDelay = 1000L;

    boolean useExponentialBackOff = true;
    double backoffMultiplier = 2.0;

    long initialRedeliveryDelay = 1000L;
    long maximumRedeliveryDelay = 60000L;
    int maximumRedeliveries = -1;

    public AbstractSplunkActiveMqConsumerRunnable(String destinationName) {
        super(destinationName);
    }

    public AbstractSplunkActiveMqConsumerRunnable(String destinationName, boolean useTopic) {
        super(destinationName, useTopic);
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

    public void setUseRedelivery(String useRedelivery) {
        this.useRedelivery = Boolean.valueOf(useRedelivery);
    }

    public void setUseRedelivery(boolean useRedelivery) {
        this.useRedelivery = useRedelivery;
    }

    public long getRedeliveryDelay() {
        return redeliveryDelay;
    }

    public void setRedeliveryDelay(String redeliveryDelay) {
        this.redeliveryDelay = Long.valueOf(redeliveryDelay);
    }

    public void setRedeliveryDelay(long redeliveryDelay) {
        this.redeliveryDelay = redeliveryDelay;
    }

    public boolean isUseExponentialBackOff() {
        return useExponentialBackOff;
    }

    public void setUseExponentialBackOff(String useExponentialBackOff) {
        this.useExponentialBackOff = Boolean.valueOf(useExponentialBackOff);
    }

    public void setUseExponentialBackOff(boolean useExponentialBackOff) {
        this.useExponentialBackOff = useExponentialBackOff;
    }

    public Double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public void setBackoffMultiplier(String backoffMultiplier) {
        this.backoffMultiplier = Double.valueOf(backoffMultiplier);
    }


    public void setBackoffMultiplier(double backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
    }

    public Long getInitialRedeliveryDelay() {
        return initialRedeliveryDelay;
    }

    public void setInitialRedeliveryDelay(String initialRedeliveryDelay) {
        this.initialRedeliveryDelay = Long.valueOf(initialRedeliveryDelay);
    }

    public void setInitialRedeliveryDelay(long initialRedeliveryDelay) {
        this.initialRedeliveryDelay = initialRedeliveryDelay;
    }

    public Long getMaximumRedeliveryDelay() {
        return maximumRedeliveryDelay;
    }

    public void setMaximumRedeliveryDelay(String maximumRedeliveryDelay) {
        this.maximumRedeliveryDelay = Long.valueOf(maximumRedeliveryDelay);
    }

    public void setMaximumRedeliveryDelay(long maximumRedeliveryDelay) {
        this.maximumRedeliveryDelay = maximumRedeliveryDelay;
    }

    public int getMaximumRedeliveries() {
        return maximumRedeliveries;
    }

    public void setMaximumRedeliveries(String maximumRedeliveries) {
        this.maximumRedeliveries = Integer.valueOf(maximumRedeliveries);
    }

    public void setMaximumRedeliveries(Integer maximumRedeliveries) {
        this.maximumRedeliveries = maximumRedeliveries;
    }

    @Override
    public ConnectionFactory getConnectionFactory() {
        return super.hasConnectionFactory() ? super.getConnectionFactory() : createConnectionFactory();
    }

    @Override
    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        if (useRedelivery && connectionFactory instanceof ActiveMQConnectionFactory) {
            ActiveMQConnectionFactory activeMQConnectionFactory = (ActiveMQConnectionFactory) connectionFactory;
            configureRedelivery(activeMQConnectionFactory);
        }
        super.setConnectionFactory(connectionFactory);
    }

    protected void configureRedelivery(ActiveMQConnectionFactory connectionFactory) {
        if (useRedelivery) {
            RedeliveryPolicy redeliveryPolicy = connectionFactory.getRedeliveryPolicy();
            if (redeliveryPolicy == null) {
                redeliveryPolicy = new RedeliveryPolicy();
                connectionFactory.setRedeliveryPolicy(redeliveryPolicy);
            }
            redeliveryPolicy.setMaximumRedeliveries(maximumRedeliveries);
            if (useExponentialBackOff) {
                redeliveryPolicy.setUseExponentialBackOff(true);
                redeliveryPolicy.setBackOffMultiplier(backoffMultiplier);
                redeliveryPolicy.setInitialRedeliveryDelay(initialRedeliveryDelay);
                redeliveryPolicy.setMaximumRedeliveryDelay(maximumRedeliveryDelay);
            } else {
                redeliveryPolicy.setUseExponentialBackOff(false);
                redeliveryPolicy.setRedeliveryDelay(redeliveryDelay);
            }
        }
    }

    protected abstract ConnectionFactory createConnectionFactory();

}
