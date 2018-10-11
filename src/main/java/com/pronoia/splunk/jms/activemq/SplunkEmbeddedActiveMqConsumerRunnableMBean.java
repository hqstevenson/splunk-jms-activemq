package com.pronoia.splunk.jms.activemq;

import java.util.Date;

public interface SplunkEmbeddedActiveMqConsumerRunnableMBean {
    String getFactoryId();

    String getEmbeddedConsumerRunnableId();

    String getBrokerName();

    String getDestinationType();

    String getDestinationName();

    boolean isConnectionStarted();

    boolean isRunning();

    Date getStartTime();
    Date getStopTime();

    long getReceiveTimeoutMillis();

    long getInitialDelaySeconds();

    long getDelaySeconds();

    Date getConnectionStartTime();

    Date getLastMessageTime();

    long getConsumedMessageCount();

    Date getConnectionStopTime();

    void start();

    void stop();

    void restart();

}
