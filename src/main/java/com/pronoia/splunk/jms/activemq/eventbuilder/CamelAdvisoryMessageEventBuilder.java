package com.pronoia.splunk.jms.activemq.eventbuilder;

import com.pronoia.splunk.eventcollector.EventBuilder;

import javax.jms.Message;

public class CamelAdvisoryMessageEventBuilder extends AdvisoryMessageEventBuilder {
  public CamelAdvisoryMessageEventBuilder() {
    setPropertyNameReplacement("_DOT_", ".");
    setPropertyNameReplacement("_HYPHEN_", "-");
  }

  @Override
  public EventBuilder<Message> duplicate() {
    CamelAdvisoryMessageEventBuilder answer = new CamelAdvisoryMessageEventBuilder();

    answer.copyConfiguration(this);

    return answer;
  }

}
