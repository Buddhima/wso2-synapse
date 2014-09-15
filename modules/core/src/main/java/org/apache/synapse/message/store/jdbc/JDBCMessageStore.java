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
package org.apache.synapse.message.store.jdbc;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.message.store.AbstractMessageStore;
import org.apache.synapse.message.store.jdbc.message.JDBCPersistentMessage;
import org.apache.synapse.message.store.jdbc.util.JDBCPersistentMessageHelper;
import org.apache.synapse.message.store.jdbc.util.JDBCUtil;
import org.apache.synapse.message.store.jdbc.util.Statement;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;


public class JDBCMessageStore extends AbstractMessageStore {

    /**
     * Message Helper use to convert messages to serializable form and backward
     */
    private JDBCPersistentMessageHelper jdbcPersistentMessageHelper;

    /**
     * Message Utility class used to provide utilities to do processing
     */
    private JDBCUtil jdbcUtil;

    private static final Log log = LogFactory.getLog(JDBCMessageStore.class);

    private Semaphore removeLock = new Semaphore(1);
    private Semaphore cleanUpOfferLock = new Semaphore(1);
    private AtomicBoolean cleaning = new AtomicBoolean(false);

    /**
     * Initializes the JDBC Message Store
     *
     * @param synapseEnvironment SynapseEnvironment
     */
    @Override
    public void init(SynapseEnvironment synapseEnvironment) {
        log.debug("Initializing JDBC Message Store");
        super.init(synapseEnvironment);

        jdbcUtil = new JDBCUtil();

        log.debug("Initializing Datasource and Properties");

        // Building the datasource using util class
        jdbcUtil.buildDataSource(parameters);

        jdbcPersistentMessageHelper = new JDBCPersistentMessageHelper(synapseEnvironment);


    }

    /**
     * Set JDBC store parameters
     *
     * @param parameters - List of parameters to set
     */
    @Override
    public void setParameters(Map<String, Object> parameters) {
        super.setParameters(parameters);

        // Rebuild utils after setting new parameters
        if (jdbcUtil != null) {
            jdbcUtil.buildDataSource(parameters);
        }

    }

    /**
     * Process a given Statement object
     *
     * @param stmt - Statement to process
     * @return - Results as a List of MessageContexts
     */
    public List<MessageContext> processStatementWithResult(Statement stmt) {

        List<MessageContext> list = new ArrayList<MessageContext>();

        // execute the prepared statement, and return list of messages as an ArrayList
        Connection con = null;
        ResultSet rs = null;
        PreparedStatement ps;

        try {
            ps = jdbcUtil.getPreparedStatement(stmt);

            for (int i = 1; i <= stmt.getParameters().size(); ++i) {
                Object param = stmt.getParameters().get(i - 1);
                if (param instanceof String) {
                    ps.setString(i, (String) stmt.getParameters().get(i - 1));
                } else if (param instanceof Integer) {
                    ps.setInt(i, (Integer) stmt.getParameters().get(i - 1));
                }
            }

            con = ps.getConnection();
            rs = ps.executeQuery();

            while (rs.next()) {

                Object msgObj;
                try {
                    msgObj = rs.getObject("message");
                } catch (Exception e) {
                    log.error("Error executing statement : " + stmt.getRawStatement() +
                            " against DataSource : " + jdbcUtil.getDSName(), e);
                    break;
                }

                if (msgObj != null) {

                    try {

                        // convert back to MessageContext and add to list
                        ObjectInputStream ios = new ObjectInputStream(new ByteArrayInputStream((byte[]) msgObj));
                        Object msg = ios.readObject();
                        if (msg instanceof JDBCPersistentMessage) {
                            JDBCPersistentMessage jdbcMsg = (JDBCPersistentMessage) msg;
                            list.add(jdbcPersistentMessageHelper.createMessageContext(jdbcMsg));
                        }
                    } catch (Exception e) {
                        log.error("Error reading object input stream: " + e.getMessage());
                    }

                } else {
                    log.error("Retrieved Object is null");
                }

            }

        } catch (SQLException e) {
            log.error("Processing Statement failed : " + stmt.getRawStatement() +
                    " against DataSource : " + jdbcUtil.getDSName(), e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ignored) {
                }
            }
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException ignore) {
                }
            }
        }

        return list;
    }

    /**
     * Process statements that do not give a ResultSet
     *
     * @param stmnt - Statement to process
     * @return - Success or Failure of the process
     */
    public boolean processStatementWithoutResult(Statement stmnt) {

        Connection con = null;
        boolean result = false;
        PreparedStatement ps = null;


        try {
            ps = jdbcUtil.getPreparedStatement(stmnt);
            for (int i = 1; i <= stmnt.getParameters().size(); ++i) {
                Object param = stmnt.getParameters().get(i - 1);
                if (param instanceof String) {
                    ps.setString(i, (String) stmnt.getParameters().get(i - 1));
                } else if (param instanceof JDBCPersistentMessage) {
                    ps.setObject(i, param);
                }
            }

            con = ps.getConnection();
            result = ps.execute();

        } catch (SQLException e) {
            log.error("Processing Statement failed : " + stmnt.getRawStatement() +
                    " against DataSource : " + jdbcUtil.getDSName(), e);
            result = false;
        } finally {

            if (con != null) {
                try {
                    con.close();
                } catch (SQLException ignore) {
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException ignore) {
                }
            }
        }

        return result;
    }

    /**
     * Destroy Resources allocated
     */
    @Override
    public void destroy() {
        super.destroy();
        jdbcPersistentMessageHelper = null;
        jdbcUtil = null;
    }

    /**
     * Add a message to the end of the table    . If fetching success return true else false
     *
     * @param messageContext message to insert
     * @return -  success/failure of fetching
     */
    @Override
    public synchronized boolean offer(MessageContext messageContext) {

        if (messageContext == null) {
            log.error("Message is null, cant offer into database");
            return false;
        }
        if (cleaning.get()) {
            try {
                cleanUpOfferLock.acquire();
            } catch (InterruptedException ie) {
                log.error("Message Cleanup lock released unexpectedly", ie);
            }
        }

        JDBCPersistentMessage persistentMessage = jdbcPersistentMessageHelper.createPersistentMessage(messageContext);
        String msg_id = persistentMessage.getJdbcPersistentAxis2Message().getMessageID();
        Statement stmt = new Statement("INSERT INTO " + jdbcUtil.getTableName() + " (msg_id,message) VALUES (?,?)");

        stmt.addParameter(msg_id);
        stmt.addParameter(persistentMessage);
        return processStatementWithoutResult(stmt);

    }

    /**
     * Poll the table. Remove & Return the first element from table
     *
     * @return - mc - MessageContext of the first element
     */
    @Override
    public MessageContext poll() {
        MessageContext mc = null;
        try {
            removeLock.acquire();
            mc = peek();
            if (mc != null) {
                long minIdx = getMinTableIndex();
                if (minIdx != 0) {
                    Statement stmt = new Statement("DELETE FROM " + jdbcUtil.getTableName() + " WHERE indexId=?");
                    stmt.addParameter(Long.toString(minIdx));
                    processStatementWithoutResult(stmt);
//                    System.out.println("del : " + stmt.getRawStatement());
                }
            }
        } catch (InterruptedException ie) {
            log.error("Message Cleanup lock released unexpectedly," +
                    "Message count value might show a wrong value ," +
                    "Restart the system to re sync the message count", ie);
        } finally {
            removeLock.release();
        }


        return mc;
    }

    /**
     * Select and return the first element in current table
     *
     * @return - Select and return the first element from the table
     */
    @Override
    public MessageContext peek() {

        Statement stmt = new Statement("SELECT message FROM " + jdbcUtil.getTableName() + " WHERE indexId=(SELECT min(indexId) from " + jdbcUtil.getTableName() + ")");
        List<MessageContext> result = processStatementWithResult(stmt);
        if (result.size() > 0) {
            return result.get(0);
        } else {
            log.debug("No first element found !");
            return null;
        }
    }

    /**
     * Removes the first element from table, same as polling
     *
     * @return MessageContext - first message context
     * @throws java.util.NoSuchElementException
     *
     */
    @Override
    public MessageContext remove() throws NoSuchElementException {
        MessageContext messageContext = poll();
        if (messageContext != null) {
            return messageContext;
        } else {
            throw new NoSuchElementException("First element not found and remove failed !");
        }
    }

    /**
     * Delete all entries from table !
     */
    @Override
    public void clear() {
        try {
            removeLock.acquire();
            cleaning.set(true);
            cleanUpOfferLock.acquire();
            Statement stmt = new Statement("DELETE FROM " + jdbcUtil.getTableName());
            processStatementWithoutResult(stmt);
        } catch (InterruptedException ie) {
            log.error("Acquiring lock failed !: " + ie.getMessage());
        } finally {
            removeLock.release();
            cleaning.set(false);
            cleanUpOfferLock.release();
        }
    }

    /**
     * Remove the message with given msg_id
     *
     * @param msg_id - message ID
     * @return - removed message context
     */
    @Override
    public MessageContext remove(String msg_id) {
        MessageContext result = null;
        try {
            removeLock.acquire();
            result = get(msg_id);
            Statement stmt = new Statement("DELETE FROM " + jdbcUtil.getTableName() + " WHERE msg_id=?");
            stmt.addParameter(msg_id);
            processStatementWithoutResult(stmt);

        } catch (InterruptedException ie) {
            log.error("Acquiring lock failed !: " + ie.getMessage());
        } finally {
            removeLock.release();
        }
        return result;
    }

    /**
     * Get the message at given position
     *
     * @param i - position of the message , starting value is 0
     * @return Message Context of i th row or if failed return null
     */
    @Override
    public MessageContext get(int i) {

        if (i < 0) {
            throw new IllegalArgumentException("Index:" + i + " out of table bound");
        }

        if (!this.getParameters().get(JDBCMessageStoreConstants.JDBC_CONNECTION_DRIVER).
                equals("com.mysql.jdbc.Driver")) {
            throw new UnsupportedOperationException("Only support in MYSQL");
        }   //
        // Gets the minimum value of the sub-table which contains indexId values greater than given i (i has minimum of 0 while indexId has minimum of 1)
        Statement stmt = new Statement("SELECT indexId,message FROM " + jdbcUtil.getTableName() + " ORDER BY indexId ASC LIMIT ?,1 ");
        stmt.addParameter(i);
        List<MessageContext> result = processStatementWithResult(stmt);

        if (result.size() > 0) {
            return result.get(0);
        } else {
            return null;
        }
    }

    /**
     * Get all messages in the table
     *
     * @return - List containing all message contexts in the store
     */
    @Override
    public List<MessageContext> getAll() {
        Statement stmt = new Statement("SELECT message FROM " + jdbcUtil.getTableName());
        return processStatementWithResult(stmt);
    }

    /**
     * Return the first element with given msg_id
     *
     * @param msg_id - Message ID
     * @return - returns the first result found else null
     */
    @Override
    public MessageContext get(String msg_id) {
        Statement stmt = new Statement("SELECT message FROM " + jdbcUtil.getTableName() + " WHERE msg_id=?");
        stmt.addParameter(msg_id);
        List<MessageContext> result = processStatementWithResult(stmt);
        if (result.size() > 0) {
            return result.get(0);
        } else {
            return null;
        }
    }

    /**
     * Return number of messages in the store
     *
     * @return size - Number of messages
     */
    public int size() {

        Connection con = null;
        ResultSet rs = null;
        int size = 0;

        Statement stmt = new Statement("SELECT COUNT(*) FROM " + jdbcUtil.getTableName());

        try {
            PreparedStatement ps = jdbcUtil.getPreparedStatement(stmt);
            con = ps.getConnection();
            rs = ps.executeQuery();

            while (rs.next()) {

                try {
                    //size = rs.getInt("count(*)");
                    size = rs.getInt(1);
                } catch (Exception e) {
                    System.out.println("Error executing statement : " + stmt.getRawStatement() +
                            " against DataSource : " + jdbcUtil.getDSName());
                    log.error("Failed to read result from Result Set");
                    break;
                }

            }

        } catch (SQLException e) {
            System.out.println("Processing Statement failed : " + stmt.getRawStatement() +
                    " against DataSource : " + jdbcUtil.getDSName());
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ignored) {
                }
            }
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException ignore) {
                }
            }
        }
        return size;
    }


    /**
     * Get the maximum indexId value in the current table
     *
     * @return - maximum index value
     */
    private long getMinTableIndex() {
        Connection con = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        long size = 0;

        Statement stmt = new Statement("SELECT min(indexId) FROM " + jdbcUtil.getTableName());

        try {
            ps = jdbcUtil.getPreparedStatement(stmt);
            con = ps.getConnection();
            rs = ps.executeQuery();

            while (rs.next()) {

                try {
                    //size = rs.getInt("count(*)");
                    size = rs.getLong(1);
                } catch (Exception e) {
                    System.out.println("No Max indexId found in : " + jdbcUtil.getDSName());
                    return 0;
                }

            }

        } catch (SQLException e) {
            log.error("Processing Statement failed : " + stmt.getRawStatement() +
                    " against DataSource : " + jdbcUtil.getDSName(), e);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException ignore) {
                }
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ignored) {
                }
            }
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException ignore) {
                }
            }
        }
        return size;

    }

    /**
     * Get the datasource
     *
     * @return current Datasource
     */
    private DataSource getDataSource() {
        return jdbcUtil.getDataSource();
    }

    private String getDriverClassName() {

        try {
            return getDataSource().getConnection().getMetaData().getDriverName();
        } catch (SQLException e) {
            log.error("Error getting database driver", e);
            return null;
        }
    }


}
