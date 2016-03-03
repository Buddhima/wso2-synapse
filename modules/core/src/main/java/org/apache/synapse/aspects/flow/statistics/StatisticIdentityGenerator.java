/*
 *  Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.synapse.aspects.flow.statistics;

import org.apache.synapse.aspects.ComponentType;
import org.apache.synapse.aspects.flow.statistics.structuring.StructuringElement;

import java.util.ArrayList;
import java.util.Stack;

public class StatisticIdentityGenerator {

    private static int id = 0;

    private static int hashCode = 0;

    private static String parent;

    private static ArrayList<StructuringElement> list = new ArrayList<>();

    private static Stack<StructuringElement> stack = new Stack<>();

    private static String lastParent;

	private static boolean branching = false;

    public static String getIdString() {
        return String.valueOf(id++);
    }

    public static void resetId() {
        System.out.println(">>>>>>>HASHCODE for: " + hashCode);
        id = 0;
        hashCode = 0;
        stack.clear();
        list.clear();
        lastParent = null;
    }

    public static void setParent(String parentName) {
        parent = parentName + "@";
    }

    public static String getIdForComponent(String name, ComponentType componentType) {
        String idString = parent + getIdString() + ":" + name;
        hashCode += idString.hashCode();
        System.out.println(idString);
        process(idString, componentType);
        return idString;
    }

    public static String getIdReferencingComponent(String name, ComponentType componentType) {
        String idString = name + "@0:" + name;
        id++;
        hashCode += idString.hashCode();
        System.out.println(idString);
        process(idString, componentType);
        return idString;
    }

    public static void reportingBranchingEvents() {
        System.out.println("Branching Happening, IF~else // Clone Targets");

        lastParent = stack.peek().getId();
        branching = true;
    }

    public static void reportingEndEvent(String name, ComponentType componentType) {
        System.out.println("Ending Component Initialization:" + name);

        // If the mediator is also a clone/switch/filter/iterate - Reset lastParent value
        if (name.contains("CloneMediator") || name.contains("SwitchMediator") || name.contains("FilterMediator") || name.contains("IterateMediator")) {
            lastParent = stack.peek().getId();
            stack.pop();

            branching = false;
        }

        // If event is a SEQ or Proxy - pop from stack, then update parent
        if (ComponentType.SEQUENCE == componentType || ComponentType.PROXYSERVICE == componentType || name.contains("InMediator") || name.contains("OutMediator")) {
            stack.pop();
            if (!stack.isEmpty()) {
                lastParent = stack.peek().getId();
            }
        }
    }

    private static void process(String name, ComponentType componentType) {
        if (ComponentType.PROXYSERVICE == componentType) {
            StructuringElement proxyElem = new StructuringElement(name, componentType, false);

            stack.push(proxyElem);
            list.add(proxyElem);
            lastParent = name;
        }

        if (ComponentType.SEQUENCE == componentType) {
            StructuringElement seqElem = new StructuringElement(name, componentType, false);

            if (stack.isEmpty()) {
                // This is directly deploying a sequence
                stack.push(seqElem);
                list.add(seqElem);
            } else {
                // There's a parent for sequence
                seqElem.setParentId(lastParent);
                stack.push(seqElem);
                list.add(seqElem);
            }
            lastParent = name;
        }

        if (ComponentType.MEDIATOR == componentType) {
            StructuringElement medElem = new StructuringElement(name, componentType, branching);
            medElem.setParentId(lastParent);
            if (stack.isEmpty()) {
                // This is not a desired situation! Mediators always lies inside a sequence
            }
            list.add(medElem);
            lastParent = name;

            // If the mediator is also a clone/switch/filter/iterate - add that to stack
            if (name.contains("CloneMediator") || name.contains("SwitchMediator") || name.contains("FilterMediator") || name.contains("IterateMediator") || name.contains("InMediator") || name.contains("OutMediator")) {
                stack.push(medElem);

            }
        }

    }
}
