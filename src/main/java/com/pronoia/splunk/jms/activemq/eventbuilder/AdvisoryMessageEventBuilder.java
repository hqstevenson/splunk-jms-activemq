package com.pronoia.splunk.jms.activemq.eventbuilder;

import com.pronoia.splunk.eventcollector.EventBuilder;
import com.pronoia.splunk.eventcollector.EventCollectorInfo;
import com.pronoia.splunk.jms.eventbuilder.JmsMessageEventBuilder;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.DataStructure;

public class AdvisoryMessageEventBuilder extends JmsMessageEventBuilder {
  static final Pattern ADVISORY_TYPE_PATTERN = Pattern.compile("topic://ActiveMQ\\.Advisory\\.([^.]+)\\..*");

  @Override
  public String getHostFieldValue() {
    return super.getHostFieldValue();
  }

  @Override
  public String getIndexFieldValue() {
    return super.getIndexFieldValue();
  }

  @Override
  public String getSourceFieldValue() {
    return super.getSourceFieldValue();
  }

  @Override
  public String getSourcetypeFieldValue() {
    return super.getSourcetypeFieldValue();
  }

  @Override
  public String getTimestampFieldValue() {
    if (hasEventBody() && !hasTimestampProperty()) {
      Message advisoryMessage = getEventBody();

      try {

        String brokerOutTime = advisoryMessage.getStringProperty("orignalBrokerOutTime");
        if (brokerOutTime != null && !brokerOutTime.isEmpty()) {
          return brokerOutTime;
        }
      } catch (JMSException jmsEx) {
        log.warn("Ignoring exception encountered retrieving 'orignalBrokerOutTime' property", jmsEx);
      }

      try {
        String brokerInTime = advisoryMessage.getStringProperty("orignalBrokerInTime");
        if (brokerInTime != null && !brokerInTime.isEmpty()) {
          return brokerInTime;
        }
      } catch (JMSException jmsEx) {
        log.warn("Ignoring exception encountered retrieving 'orignalBrokerInTime' property", jmsEx);
      }
    }

    return super.getTimestampFieldValue();
  }

  @Override
  protected void addEventBodyToMap(Map<String, Object> eventObject) {
    Map<String, String> advisedProperties = new HashMap<>();

    Message jmsMessage = getEventBody();
    if (jmsMessage instanceof ActiveMQMessage) {
      ActiveMQMessage advisoryMessage = (ActiveMQMessage) jmsMessage;
      log.debug("Processing advisory message {}", advisoryMessage);
      DataStructure dataStructure = advisoryMessage.getDataStructure();
      if (dataStructure != null && dataStructure instanceof ActiveMQMessage) {
        ActiveMQMessage advisedMessage = (ActiveMQMessage) dataStructure;
        addField("orignalBrokerInTime", String.valueOf(advisedMessage.getBrokerInTime()));
        addField("orignalBrokerOutTime", String.valueOf(advisedMessage.getBrokerOutTime()));
        try {
          Enumeration<String> propertyNames = advisedMessage.getPropertyNames();
          if (propertyNames != null) {
            while (propertyNames.hasMoreElements()) {
              String propertyName = propertyNames.nextElement();
              /*
              if (!"breadcrumbId".equals(propertyName)) {
              }
              */
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
                String logMessage = String.format("Exception encountered getting property value for property name '{}' - ignoring", propertyName);
                log.warn(logMessage, getObjectPropertyEx);
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

  @Override
  protected void extractMessageHeadersToMap(Message jmsMessage, Map<String, Object> targetMap) {
    if (jmsMessage != null && targetMap != null) {
    super.extractMessageHeadersToMap(jmsMessage, targetMap);
      final String logMessageFormat = "Error Reading JMS Message Header '{}' - ignoring";

      try {
        Destination jmsDestination = jmsMessage.getJMSDestination();
        if (jmsDestination != null) {
          Matcher advisoryTypeMatcher = ADVISORY_TYPE_PATTERN.matcher(jmsDestination.toString());
          if (advisoryTypeMatcher.matches()) {
            addField("AdvisoryType", advisoryTypeMatcher.group(1));
          }
        }
      } catch (JMSException e) {
        e.printStackTrace();
      }
      if (jmsMessage instanceof ActiveMQMessage) {
        ActiveMQMessage advisoryMessage = (ActiveMQMessage) jmsMessage;

        long brokerOutTime = advisoryMessage.getBrokerOutTime();
        if (brokerOutTime > 0) {
          setTimestamp(brokerOutTime);
        } else {
          setTimestamp(advisoryMessage.getBrokerInTime());
        }

      } else {
        throw new IllegalStateException("Cannot generate Splunk event from type " + jmsMessage.getClass());
      }
    }
  }

  @Override
  public EventBuilder<Message> duplicate() {
    AdvisoryMessageEventBuilder answer = new AdvisoryMessageEventBuilder();

    answer.copyConfiguration(this);

    return answer;
  }
}
