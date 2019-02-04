package com.pronoia.splunk.jms.activemq;

import java.util.Date;
import java.util.Set;

public interface SplunkEmbeddedActiveMqConsumerRunnableMBean {
    String getFactoryId();

    String getSplunkClientId();

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

    Set<Integer> getConsumedHttpStatusCodes();

    Set<Integer> getConsumedSplunkStatusCodes();

    Date getConnectionStartTime();

    Date getLastMessageTime();

    long getConsumedMessageCount();

    Date getConnectionStopTime();

    boolean getSkipNextMessage();
    void setSkipNextMessage(boolean skipNextMessage);

    void skipNextMessage();

    void start();

    void stop();

    void restart();

}
