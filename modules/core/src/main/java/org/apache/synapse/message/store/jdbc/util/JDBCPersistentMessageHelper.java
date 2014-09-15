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
package org.apache.synapse.message.store.jdbc.util;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.impl.builder.StAXBuilder;
import org.apache.axiom.om.util.StAXUtils;
import org.apache.axiom.soap.SOAP12Constants;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axiom.soap.SOAPFactory;
import org.apache.axiom.soap.impl.builder.StAXSOAPModelBuilder;
import org.apache.axis2.addressing.EndpointReference;
import org.apache.axis2.addressing.RelatesTo;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.OperationContext;
import org.apache.axis2.context.ServiceContext;
import org.apache.axis2.context.ServiceGroupContext;
import org.apache.axis2.description.AxisOperation;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.core.axis2.Axis2SynapseEnvironment;
import org.apache.synapse.message.store.jdbc.message.JDBCPersistentAxis2Message;
import org.apache.synapse.message.store.jdbc.message.JDBCPersistentMessage;
import org.apache.synapse.message.store.jdbc.message.JDBCPersistentSynapseMessage;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;


public class JDBCPersistentMessageHelper {

    private SynapseEnvironment synapseEnvironment;

    private Log log = LogFactory.getLog(JDBCPersistentMessage.class);

    public JDBCPersistentMessageHelper(SynapseEnvironment se) {
        this.synapseEnvironment = se;
    }

    public MessageContext createMessageContext(JDBCPersistentMessage message) {

        SynapseConfiguration configuration = synapseEnvironment.getSynapseConfiguration();
        MessageContext synCtx=null;
        org.apache.axis2.context.MessageContext msgCtx = ((Axis2SynapseEnvironment)
                                                                  synapseEnvironment).getAxis2ConfigurationContext().createMessageContext();


        AxisConfiguration axisConfiguration = msgCtx.getConfigurationContext().getAxisConfiguration();
        JDBCPersistentAxis2Message jdbcAxis2MessageContext = message.getJdbcPersistentAxis2Message();
        SOAPEnvelope envelope = getSoapEnvelope(jdbcAxis2MessageContext.getSoapEnvelope());

        try {

            msgCtx.setEnvelope(envelope);
            // set the  properties
            msgCtx.getOptions().setAction(jdbcAxis2MessageContext.getAction());
            if (jdbcAxis2MessageContext.getRelatesToMessageId() != null) {
                msgCtx.addRelatesTo(new RelatesTo(jdbcAxis2MessageContext.getRelatesToMessageId()));
            }
            msgCtx.setMessageID(jdbcAxis2MessageContext.getMessageID());
            msgCtx.getOptions().setAction(jdbcAxis2MessageContext.getAction());

            AxisService axisService =
                    axisConfiguration.getServiceForActivation(jdbcAxis2MessageContext.getService());

            AxisOperation axisOperation =
                    axisService.getOperation(jdbcAxis2MessageContext.getOperationName());

            msgCtx.setFLOW(jdbcAxis2MessageContext.getFLOW());
            ArrayList executionChain = new ArrayList();
            if (jdbcAxis2MessageContext.getFLOW() ==
                org.apache.axis2.context.MessageContext.OUT_FLOW) {
                executionChain.addAll(axisOperation.getPhasesOutFlow());
                executionChain.addAll(axisConfiguration.getOutFlowPhases());

            } else if (jdbcAxis2MessageContext.getFLOW() ==
                       org.apache.axis2.context.MessageContext.OUT_FAULT_FLOW) {
                executionChain.addAll(axisOperation.getPhasesOutFaultFlow());
                executionChain.addAll(axisConfiguration.getOutFlowPhases());
            }

            msgCtx.setExecutionChain(executionChain);


            ConfigurationContext configurationContext = msgCtx.getConfigurationContext();

            msgCtx.setAxisService(axisService);
            ServiceGroupContext serviceGroupContext =
                    configurationContext.createServiceGroupContext(axisService.getAxisServiceGroup());
            ServiceContext serviceContext = serviceGroupContext.getServiceContext(axisService);

            OperationContext operationContext = serviceContext.createOperationContext(
                    jdbcAxis2MessageContext.getOperationName());
            msgCtx.setServiceContext(serviceContext);
            msgCtx.setOperationContext(operationContext);

            msgCtx.setAxisService(axisService);
            msgCtx.setAxisOperation(axisOperation);
            if (jdbcAxis2MessageContext.getReplyToAddress() != null) {
                msgCtx.setReplyTo(new EndpointReference(jdbcAxis2MessageContext.getReplyToAddress().trim()));
            }

            if (jdbcAxis2MessageContext.getFaultToAddress() != null) {
                msgCtx.setFaultTo(new EndpointReference(jdbcAxis2MessageContext.getFaultToAddress().trim()));
            }

            if (jdbcAxis2MessageContext.getFromAddress() != null) {
                msgCtx.setFrom(new EndpointReference(jdbcAxis2MessageContext.getFromAddress().trim()));
            }

            if (jdbcAxis2MessageContext.getToAddress() != null) {
                msgCtx.getOptions().setTo(new EndpointReference(jdbcAxis2MessageContext.getToAddress().trim()));
            }

            msgCtx.setProperties(jdbcAxis2MessageContext.getProperties());
            msgCtx.setTransportIn(axisConfiguration.
                    getTransportIn(jdbcAxis2MessageContext.getTransportInName()));
            msgCtx.setTransportOut(axisConfiguration.
                    getTransportOut(jdbcAxis2MessageContext.getTransportOutName()));

            JDBCPersistentSynapseMessage jdbcSynpaseMessageContext
                    = message.getJdbcPersistentSynapseMessage();

            synCtx =
                    new Axis2MessageContext(msgCtx, configuration, synapseEnvironment);
            synCtx.setTracingState(jdbcSynpaseMessageContext.getTracingState());

            Iterator<String> it = jdbcSynpaseMessageContext.getProperties().keySet().iterator();

            while (it.hasNext()) {
                String key = it.next();
                Object value = jdbcSynpaseMessageContext.getProperties().get(key);

                synCtx.setProperty(key, value);
            }

            synCtx.setFaultResponse(jdbcSynpaseMessageContext.isFaultResponse());
            synCtx.setResponse(jdbcSynpaseMessageContext.isResponse());


        } catch (Exception e) {
            log.error("Error while deserializing the JDBC Persistent Message " + e);
//            return null;
        }
        return synCtx;
    }

    public JDBCPersistentMessage createPersistentMessage(MessageContext synCtx) {

        JDBCPersistentMessage jdbcMsg = new JDBCPersistentMessage();
        JDBCPersistentAxis2Message jdbcAxis2MessageContext = new JDBCPersistentAxis2Message();
        JDBCPersistentSynapseMessage jdbcSynpaseMessageContext = new JDBCPersistentSynapseMessage();

        Axis2MessageContext axis2MessageContext = null;
        if (synCtx instanceof Axis2MessageContext) {

            /**
             * Serializing the Axis2 Message Context
             */
        try{
            axis2MessageContext = (Axis2MessageContext) synCtx;
            org.apache.axis2.context.MessageContext msgCtx =
                    axis2MessageContext.getAxis2MessageContext();

//            jdbcAxis2MessageContext.setMessageID(UUIDGenerator.getUUID());
            jdbcAxis2MessageContext.setMessageID(msgCtx.getMessageID());
            jdbcAxis2MessageContext.setOperationAction(msgCtx.getAxisOperation().getSoapAction());
            jdbcAxis2MessageContext.setOperationName(msgCtx.getAxisOperation().getName());

            jdbcAxis2MessageContext.setAction(msgCtx.getOptions().getAction());
            jdbcAxis2MessageContext.setService(msgCtx.getAxisService().getName());

            if (msgCtx.getRelatesTo() != null) {
                jdbcAxis2MessageContext.setRelatesToMessageId(msgCtx.getRelatesTo().getValue());
            }
            if (msgCtx.getReplyTo() != null) {
                jdbcAxis2MessageContext.setReplyToAddress(msgCtx.getReplyTo().getAddress());
            }
            if (msgCtx.getFaultTo() != null) {
                jdbcAxis2MessageContext.setFaultToAddress(msgCtx.getFaultTo().getAddress());
            }
            if (msgCtx.getTo() != null) {
                jdbcAxis2MessageContext.setToAddress(msgCtx.getTo().getAddress());
            }

            jdbcAxis2MessageContext.setDoingPOX(msgCtx.isDoingREST());
            jdbcAxis2MessageContext.setDoingMTOM(msgCtx.isDoingMTOM());
            jdbcAxis2MessageContext.setDoingSWA(msgCtx.isDoingSwA());

            String soapEnvelope = msgCtx.getEnvelope().toString();
            jdbcAxis2MessageContext.setSoapEnvelope(soapEnvelope);
            jdbcAxis2MessageContext.setFLOW(msgCtx.getFLOW());


            jdbcAxis2MessageContext.setTransportInName(msgCtx.getTransportIn().getName());
            jdbcAxis2MessageContext.setTransportOutName(msgCtx.getTransportOut().getName());
            Iterator<String> it = msgCtx.getProperties().keySet().iterator();

            while (it.hasNext()) {
                String key = it.next();
                Object v = msgCtx.getProperty(key);

                String value = null;

                if (v != null) {
                    value = v.toString();
                }

                jdbcAxis2MessageContext.addProperty(key, value);
            }

        }catch (Exception e){
            log.warn("Incomplete Serialized Message !");
        }
            jdbcMsg.setJdbcPersistentAxis2Message(jdbcAxis2MessageContext);


            jdbcSynpaseMessageContext.setFaultResponse(synCtx.isFaultResponse());
            jdbcSynpaseMessageContext.setTracingState(synCtx.getTracingState());
            jdbcSynpaseMessageContext.setResponse(synCtx.isResponse());


            Iterator<String> its = synCtx.getPropertyKeySet().iterator();
            while (its.hasNext()) {

                String key = its.next();
                Object v = synCtx.getProperty(key);

                String value = null;

                if (v != null) {
                    value = v.toString();
                }
                jdbcSynpaseMessageContext.addPropertie(key, value);

            }

            jdbcMsg.setJdbcPersistentSynapseMessage(jdbcSynpaseMessageContext);

        } else {
            throw new SynapseException("Only Axis2 Messages are supported with JDBCMessage store");
        }


        return jdbcMsg;
    }

    private SOAPEnvelope getSoapEnvelope(String soapEnvelpe) {
        try {
            XMLStreamReader xmlReader =
                    StAXUtils.createXMLStreamReader(new ByteArrayInputStream(getUTF8Bytes(soapEnvelpe)));
            StAXBuilder builder = new StAXSOAPModelBuilder(xmlReader);
            SOAPEnvelope soapEnvelope = (SOAPEnvelope) builder.getDocumentElement();
            soapEnvelope.build();
            String soapNamespace = soapEnvelope.getNamespace().getNamespaceURI();
            if (soapEnvelope.getHeader() == null) {
                SOAPFactory soapFactory = null;
                if (soapNamespace.equals(SOAP12Constants.SOAP_ENVELOPE_NAMESPACE_URI)) {
                    soapFactory = OMAbstractFactory.getSOAP12Factory();
                } else {
                    soapFactory = OMAbstractFactory.getSOAP11Factory();
                }
                soapFactory.createSOAPHeader(soapEnvelope);
            }
            return soapEnvelope;
        } catch (XMLStreamException e) {
            log.error("Error while deserializing the SOAP " + e);
            return null;
        }
    }

    private byte[] getUTF8Bytes(String soapEnvelpe) {
        byte[] bytes = null;
        try {
            bytes = soapEnvelpe.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            log.error("Unable to extract bytes in UTF-8 encoding. "
                      + "Extracting bytes in the system default encoding"
                      + e.getMessage());
            bytes = soapEnvelpe.getBytes();
        }
        return bytes;
    }


}
