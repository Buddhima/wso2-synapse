/**
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.synapse.aspects.flow.statistics.structuring;

import org.apache.synapse.aspects.ComponentType;

import java.util.ArrayList;
import java.util.List;

public class StructuringElement {
//    private String name;
    private String id;
    private ComponentType type;

    private String parentId;

    List<StructuringElement> children;

    public StructuringElement(String id, ComponentType type) {
        children = new ArrayList<StructuringElement>();
        this.id = id;
        this.type = type;
    }

    public boolean addChild(StructuringElement element) {
        return children.add(element);
    }

    public String getId() {
        return id;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }
}
