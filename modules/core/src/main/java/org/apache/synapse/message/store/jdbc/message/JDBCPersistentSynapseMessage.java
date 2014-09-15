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

import org.apache.synapse.SynapseConstants;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class JDBCPersistentSynapseMessage implements Serializable {

    private ArrayList<String> localEntries = new ArrayList<String>();


    private HashMap<String, String> properties = new HashMap<String, String>();

    private boolean response = false;

    private boolean faultResponse = false;

    private int tracingState = SynapseConstants.TRACING_UNSET;

    public boolean isResponse() {
        return response;
    }

    public void setResponse(boolean response) {
        this.response = response;
    }

    public boolean isFaultResponse() {
        return faultResponse;
    }

    public void setFaultResponse(boolean faultResponse) {
        this.faultResponse = faultResponse;
    }

    public int getTracingState() {
        return tracingState;
    }

    public void setTracingState(int tracingState) {
        this.tracingState = tracingState;
    }

    public List<String> getLocalEntries() {
        return localEntries;
    }

    public HashMap<String, String> getProperties() {
        return properties;
    }

    public void addPropertie(String key, String value) {
        properties.put(key, value);
    }

    public void addLocalEntry(String key) {
        localEntries.add(key);
    }
}
