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
package com.pronoia.splunk.jms.eventbuilder;

import com.pronoia.splunk.eventcollector.EventCollectorClient;
import com.pronoia.splunk.jms.activemq.eventbuilder.AdvisoryMessageEventBuilder;
import com.pronoia.stub.httpec.EventCollectorClientStub;
import com.pronoia.stub.jms.activemq.AdvisoryMessageStub;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


/**
 * Tests for the JmsMessageEventBuilder with TextMessages class.
 */
@Ignore("AdvisoryMessageEventBuilderMessageHeadersTest not yet implemented")
public class AdvisoryMessageEventBuilderMessageHeadersTest {
    EventCollectorClient clientStub = new EventCollectorClientStub();

    AdvisoryMessageEventBuilder instance;
    AdvisoryMessageStub message = new AdvisoryMessageStub();


    @Before
    public void setUp() throws Exception {
        instance = new AdvisoryMessageEventBuilder();
    }

    /**
     * Description of test.
     *
     * @throws Exception in the event of a test error.
     */
    @Test
    public void testBuild() throws Exception {
        // @formatter:off
        String expected =
                "{"
                + "\"host\":\"macpro.local\","
                + "\"time\":\"1507221854753\","
                + "\"fields\":{"
                +     "\"JMSDeliveryMode\":\"NON_PERSISTENT\","
                +     "\"JMSMessageID\":\"ID:Dummy-AdvisoryMessage-ID\","
                +     "\"JMSType\":\"com.pronoia.stub.jms.activemq.AdvisoryMessageStub\","
                +     "\"JMSRedelivered\":\"false\","
                +     "\"orignalBrokerInTime\":\"1507221854653\","
                +     "\"orignalBrokerOutTime\":\"1507221854753\","
                +     "\"breadcrumbId\":\"dummy-breadcrumb-id\","
                +     "\"orignalMessageId\":\"Dummy-Message-ID\","
                +     "\"orignalDestination\":\"dummy-destination\","
                +     "\"orignalBrokerName\":\"dummy-broker-name\""
                +   "},"
                + "\"event\":{"
                +     "\"breadcrumbId\":\"dummy-breadcrumb-id\""
                +   "}"
                + "}";
        // @formatter:on

        instance.setEventBody(message);

        assertEquals(expected, instance.build(clientStub));
    }

    /**
     * Description of test.
     *
     * @throws Exception in the event of a test error.
     */
    @Ignore("Test for serializeBody method not yet implemented")
    @Test
    public void testserializeBody() throws Exception {
        throw new UnsupportedOperationException("testserializeBody not yet implemented");
    }

}