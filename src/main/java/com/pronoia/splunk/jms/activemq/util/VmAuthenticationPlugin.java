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
package com.pronoia.splunk.jms.activemq.util;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class VmAuthenticationPlugin implements BrokerPlugin {
    public static final String DEFAULT_VM_USER = "splunk";
    public static final String DEFAULT_VM_GROUP = "users";

    final Logger log = LoggerFactory.getLogger(this.getClass());

    String vmUser = DEFAULT_VM_USER;
    String vmGroup = DEFAULT_VM_GROUP;

    @Override
    public Broker installPlugin(Broker parent) throws Exception {
        log.warn("Installing .....");
        VmAuthenticationBroker vmAuthenticationBroker = new VmAuthenticationBroker(parent);

        vmAuthenticationBroker.setVmUser(vmUser);
        vmAuthenticationBroker.setVmGroup(vmGroup);

        return vmAuthenticationBroker;
    }

    public String getVmUser() {
        return vmUser;
    }

    public void setVmUser(String vmUser) {
        this.vmUser = vmUser;
    }

    public String getVmGroup() {
        return vmGroup;
    }

    public void setVmGroup(String vmGroup) {
        this.vmGroup = vmGroup;
    }
}
