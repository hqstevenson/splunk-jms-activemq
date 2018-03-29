/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pronoia.splunk.jms.activemq.util;

import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.Connector;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.activemq.security.AbstractAuthenticationBroker;
import org.apache.activemq.security.SecurityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class VmAuthenticationBroker extends AbstractAuthenticationBroker {
    final Logger log = LoggerFactory.getLogger(this.getClass());

    String vmUser;
    String vmGroup;

    public VmAuthenticationBroker(Broker next) {
        super(next);
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

    @Override
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        SecurityContext s = context.getSecurityContext();
        if (s == null) {
            if (isAuthorizedVmConnection(context, info)) {
                info.setUserName(vmUser);
                s = new SecurityContext(info.getUserName()) {
                    @Override
                    public Set<Principal> getPrincipals() {
                        Set<Principal> groups = new HashSet<>();
                        groups.add(new GroupPrincipal(vmGroup));
                        return groups;
                    }
                };
            }

            context.setSecurityContext(s);
            securityContexts.add(s);
        }

        try {
            super.addConnection(context, info);
        } catch (Exception e) {
            securityContexts.remove(s);
            context.setSecurityContext(null);
            throw e;
        }
    }

    boolean isAuthorizedVmConnection(ConnectionContext connectionContext, ConnectionInfo connectionInfo) {
        if (connectionInfo.getUserName() == null && connectionInfo.getPassword() == null) {
            Connector connector = connectionContext.getConnector();
            if (connector instanceof TransportConnector) {
                TransportConnector transportConnector = (TransportConnector) connector;
                String connectorName = transportConnector.getName();
                if (connectorName != null && !connectorName.isEmpty()) {
                    if (connectorName.startsWith("vm://")) {
                        log.info("isAuthorizedVmConnection(ConnectionContext, ConnectionInfo) returning true for connectorName={}", connectorName);
                        return true;
                    }
                }
            }
        }

        return false;
    }
}
