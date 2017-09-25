package com.pronoia.splunk.jms.activemq;

import com.pronoia.splunk.jms.SplunkJmsMessageConsumer;
import com.pronoia.splunk.jms.activemq.internal.ActiveMqBrokerUtils;
import org.apache.activemq.ConnectionFailedException;

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
