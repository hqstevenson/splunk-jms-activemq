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

package com.pronoia.stub.jms.activemq;

import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTextMessage;

public class AdvisoryMessageStub extends ActiveMQMessage {

  public AdvisoryMessageStub() {
    final String advisedMessageId = "Dummy-Message-ID";

    try {
      this.setJMSMessageID("Dummy-AdvisoryMessage-ID");
    } catch (JMSException jmsEx) {
      throw new IllegalStateException("Failed to set JMS Message ID");
    }

    this.setBrokerOutTime(System.currentTimeMillis());
    this.setBrokerInTime(this.getBrokerOutTime() - 10);

    try {
      setProperty("orignalMessageId", advisedMessageId);
      setProperty("orignalBrokerName", "dummy-broker-name");
      setProperty("orignalDestination", "dummy-destination");
    } catch (IOException e) {
      throw new IllegalStateException("Failed to set property on advisory message");
    }

    Enumeration propertyNames = null;

    try {
      propertyNames = this.getPropertyNames();
    } catch (JMSException e) {
      e.printStackTrace();
    }

    ActiveMQTextMessage advisedMessage = new ActiveMQTextMessage();

    try {
      advisedMessage.setJMSMessageID(advisedMessageId);
    } catch (JMSException e) {
      throw new IllegalStateException("Failed to set JMS Message ID");
    }
    advisedMessage.setBrokerOutTime(1507221854753L);
    advisedMessage.setBrokerInTime(advisedMessage.getBrokerOutTime() - 100);
    advisedMessage.setJMSTimestamp(advisedMessage.getBrokerInTime());

    try {
      advisedMessage.setProperty("breadcrumbId", "dummy-breadcrumb-id");
    } catch (IOException e) {
      throw new IllegalStateException("Failed to set property on advised message");
    }

    this.setDataStructure(advisedMessage);
  }

  }
