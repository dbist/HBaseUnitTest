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

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * @author aervits
 * 
You can use Mockito to do unit testing on smaller units. 
For instance, you can mock a org.apache.hadoop.hbase.Server instance 
or a org.apache.hadoop.hbase.master.MasterServices interface reference 
rather than a full-blown org.apache.hadoop.hbase.master.HMaster.
*/
@RunWith(MockitoJUnitRunner.class)
public class MockitoHBaseDAOTest {

    private static final Logger LOG = Logger.getLogger(MockitoHBaseDAOTest.class.getName());

    public MockitoHBaseDAOTest() {
        try {
            this.connection = ConnectionFactory.createConnection(config);
        } catch (IOException ex) {
            Logger.getLogger(MockitoHBaseDAOTest.class.getName()).log(Level.SEVERE, null, ex);
        }
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

    // Mockito Unit Test
    //
    @Mock
    Configuration config = HBaseConfiguration.create();
    @Mock
    Connection connection;
    @Mock
    private Table table;
    @Captor
    private ArgumentCaptor putCaptor;

    @Test
    public void testInsertRecord() throws Exception {
        //return mock table when getTable is called
        when(connection.getTable(TableName.valueOf("tablename"))).thenReturn(table);
        //create test object and make a call to the DAO that needs testing
        HBaseTestObj obj = new HBaseTestObj("testKey", "testValue");
        MyHBaseDAO.insertRecord(table, obj);
        verify(table).put((Put) putCaptor.capture());
        Put put = (Put) putCaptor.getValue();

        assertEquals(Bytes.toString(put.getRow()), obj.getRowKey());
        assert (put.has(Bytes.toBytes("CF"), Bytes.toBytes("CQ-1")));
        assertEquals(Bytes.toString(put.get(Bytes.toBytes("CF"), Bytes.toBytes("CQ-1")).get(0).getValue()), "testValue");
        
        LOG.log(Level.INFO, obj.getRowKey());
        LOG.log(Level.INFO, Bytes.toString(put.get(Bytes.toBytes("CF"), Bytes.toBytes("CQ-1")).get(0).getValue()), "testValue");
    }
}
