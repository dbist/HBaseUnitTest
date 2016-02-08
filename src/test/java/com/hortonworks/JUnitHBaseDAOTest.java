/*
 * Copyright 2016 aervits.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 * @author aervits
 *
 * These tests ensure that your createPut method creates, populates, and returns a Put object with expected values. 
 */

public class JUnitHBaseDAOTest {

    private static final Logger LOG = Logger.getLogger(JUnitHBaseDAOTest.class.getName());

    public JUnitHBaseDAOTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of insertRecord method, of class MyHBaseDAO.
     *
     * @throws java.lang.Exception
     */
    //TODO
    @Ignore
    public void testInsertRecord() throws Exception {
        System.out.println("insertRecord");
        Table table = null;
        HBaseTestObj obj = null;
        MyHBaseDAO.insertRecord(table, obj);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    @Test
    public void testCreatePut() throws Exception {
        HBaseTestObj obj = new HBaseTestObj("testKey", "testValue");
        Put put = MyHBaseDAO.createPut(obj);
        assertEquals(obj.getRowKey(), Bytes.toString(put.getRow()));
        assertEquals(obj.getValue(), Bytes.toString(put.get(Bytes.toBytes("CF"), Bytes.toBytes("CQ-1")).get(0).getValue()));

        LOG.log(Level.INFO, new String(put.getRow()));
        LOG.log(Level.INFO, Bytes.toString(put.get(Bytes.toBytes("CF"), Bytes.toBytes("CQ-1")).get(0).getValue()));
    }
}
