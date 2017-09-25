package com.pronoia.splunk.jms.activemq;

import com.pronoia.splunk.jms.SplunkJmsMessageConsumer;
import com.pronoia.splunk.jms.activemq.internal.ActiveMqBrokerUtils;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 *
 */
public class SplunkActiveMqMessageConsumer extends SplunkJmsMessageConsumer {
  String brokerURL;
  String userName;
  String password;

  public SplunkActiveMqMessageConsumer() {
  }

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
