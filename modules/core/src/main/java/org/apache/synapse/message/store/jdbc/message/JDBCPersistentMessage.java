/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *   * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.synapse.message.store.jdbc.message;

import java.io.Serializable;

/**
 * Serializable from of the Message store Message
 */
public class JDBCPersistentMessage implements Serializable {

    private JDBCPersistentAxis2Message jdbcPersistentAxis2Message;

    private JDBCPersistentSynapseMessage jdbcPersistentSynapseMessage;

    public JDBCPersistentAxis2Message getJdbcPersistentAxis2Message() {
        return jdbcPersistentAxis2Message;
    }

    public void setJdbcPersistentAxis2Message(
            JDBCPersistentAxis2Message jdbcPersistentAxis2Message) {
        this.jdbcPersistentAxis2Message = jdbcPersistentAxis2Message;
    }

    public JDBCPersistentSynapseMessage getJdbcPersistentSynapseMessage() {
        return jdbcPersistentSynapseMessage;
    }

    public void setJdbcPersistentSynapseMessage(
            JDBCPersistentSynapseMessage jdbcPersistentSynapseMessage) {
        this.jdbcPersistentSynapseMessage = jdbcPersistentSynapseMessage;
    }
}
