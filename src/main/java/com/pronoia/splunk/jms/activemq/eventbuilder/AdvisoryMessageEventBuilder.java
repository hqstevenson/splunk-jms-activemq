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
package com.pronoia.splunk.jms.activemq.eventbuilder;

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

import com.pronoia.splunk.eventcollector.EventBuilder;
import com.pronoia.splunk.eventcollector.EventCollectorInfo;
import com.pronoia.splunk.eventcollector.SplunkMDCHelper;

import com.pronoia.splunk.jms.eventbuilder.JmsMessageEventBuilder;

import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.DataStructure;


public class AdvisoryMessageEventBuilder extends JmsMessageEventBuilder {
    static final Pattern ADVISORY_TYPE_PATTERN = Pattern.compile("topic://ActiveMQ\\.Advisory\\.([^.]+)\\..*");

    @Override
    protected void extractMessageHeadersToMap(Message jmsMessage, Map<String, Object> targetMap) {
        if (jmsMessage != null && targetMap != null) {
            try (SplunkMDCHelper helper = createMdcHelper()) {
                super.extractMessageHeadersToMap(jmsMessage, targetMap);
                try {
                    Destination jmsDestination = jmsMessage.getJMSDestination();
                    if (jmsDestination != null) {
                        Matcher advisoryTypeMatcher = ADVISORY_TYPE_PATTERN.matcher(jmsDestination.toString());
                        if (advisoryTypeMatcher.matches()) {
                            targetMap.put("AdvisoryType", advisoryTypeMatcher.group(1));
                        }
                    }
                } catch (JMSException jmsEx) {
                    log.warn("Innoring exception encountered while attempting to derive AdvisoryType", jmsEx);
                }
                if (jmsMessage instanceof ActiveMQMessage) {
                    ActiveMQMessage advisoryMessage = (ActiveMQMessage) jmsMessage;
                    DataStructure dataStructure = advisoryMessage.getDataStructure();
                    if (dataStructure instanceof ActiveMQMessage) {
                        ActiveMQMessage advisedMessage = (ActiveMQMessage) dataStructure;

                        targetMap.put("orignalBrokerInTime", String.valueOf(advisedMessage.getBrokerInTime()));
                        targetMap.put("orignalBrokerOutTime", String.valueOf(advisedMessage.getBrokerOutTime()));

                        String propertyName = null;
                        try {
                            propertyName = "breadcrumbId";
                            Object propertyValue = advisedMessage.getProperty(propertyName);
                            if (propertyValue != null) {
                                targetMap.put(propertyName, propertyValue.toString());
                            }
                        } catch (IOException ioEx) {
                            String warningMessage = String.format("Ignoring exception encounted trying to read property %s", propertyName);
                            log.warn(warningMessage, ioEx);
                        }
                    }
                }
            }
        }
    }

    @Override
    public EventBuilder<Message> duplicate() {
        AdvisoryMessageEventBuilder answer = new AdvisoryMessageEventBuilder();

        answer.copyConfiguration(this);

        return answer;
    }

    @Override
    public String getTimestampFieldValue() {
        if (hasEventBody() && !hasTimestampProperty()) {
            Message jmsMessage = getEventBody();
            if (jmsMessage instanceof ActiveMQMessage) {
                ActiveMQMessage advisoryMessage = (ActiveMQMessage) jmsMessage;
                DataStructure dataStructure = advisoryMessage.getDataStructure();
                if (dataStructure instanceof ActiveMQMessage) {
                    ActiveMQMessage advisedMessage = (ActiveMQMessage) dataStructure;
                    long timestampMillis = advisedMessage.getBrokerOutTime();
                    if (timestampMillis <= 0) {
                        timestampMillis = advisedMessage.getBrokerInTime();
                    }

                    if (timestampMillis > 0) {
                        String timestampString = String.valueOf(timestampMillis);
                        return timestampString;
                    }
                }
            }
        }

        return super.getTimestampFieldValue();
    }

    @Override
    protected void addEventBodyToMap(Map<String, Object> eventObject) {
        Map<String, String> advisedProperties = new HashMap<>();

        try (SplunkMDCHelper helper = createMdcHelper()) {
            Message jmsMessage = getEventBody();
            if (jmsMessage instanceof ActiveMQMessage) {
                ActiveMQMessage advisoryMessage = (ActiveMQMessage) jmsMessage;
                log.debug("Processing advisory message {}", advisoryMessage);
                DataStructure dataStructure = advisoryMessage.getDataStructure();
                if (dataStructure != null && dataStructure instanceof ActiveMQMessage) {
                    ActiveMQMessage advisedMessage = (ActiveMQMessage) dataStructure;
                    try {
                        @SuppressWarnings("unchecked")
                        Enumeration<String> propertyNames = advisedMessage.getPropertyNames();
                        if (propertyNames != null) {
                            while (propertyNames.hasMoreElements()) {
                                String propertyName = propertyNames.nextElement();
                                try {
                                    Object propertyValue = advisedMessage.getObjectProperty(propertyName);
                                    if (propertyValue != null) {
                                        String propertyStringValue = propertyValue.toString();
                                        if (!propertyStringValue.isEmpty()) {
                                            if (hasPropertyNameReplacements()) {
                                                for (Map.Entry<String, String> replacementEntry : getPropertyNameReplacements().entrySet()) {
                                                    propertyName = propertyName.replaceAll(replacementEntry.getKey(), replacementEntry.getValue());
                                                }
                                            }
                                            log.debug("Adding field for property {} = {}", propertyName, propertyStringValue);
                                            advisedProperties.put(propertyName, propertyStringValue);
                                        }
                                    }
                                } catch (JMSException getObjectPropertyEx) {
                                    log.warn("Exception encountered getting property value for property name '{}' - ignoring",
                                            propertyName, getObjectPropertyEx);
                                }
                            }
                        }
                    } catch (JMSException getPropertyNamesEx) {
                        String logMessage = String.format("Exception encountered getting property names - ignoring");
                        log.warn(logMessage, getPropertyNamesEx);
                    }
                    eventObject.put(EventCollectorInfo.EVENT_BODY_KEY, advisedProperties);
                }
            } else {
                super.addEventBodyToMap(eventObject);
            }
        }
    }
}
