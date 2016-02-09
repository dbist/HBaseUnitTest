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
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author aervits
 * 
 * This code creates an HBase mini-cluster and starts it. Next, it creates a table called MyTest with one column family, CF. A record is inserted, a Get is performed from the same table, and the insertion is verified.
 */
public class IntegrationTestHBaseDAO {
    private static final Logger LOG = Logger.getLogger(IntegrationTestHBaseDAO.class.getName());

    private static HBaseTestingUtility utility;
    byte[] CF = "CF".getBytes();
    byte[] QUALIFIER = "CQ-1".getBytes();

    @Before
    public void setup() throws Exception {
        utility = new HBaseTestingUtility();
        utility.startMiniCluster();
    }

    @Test
    public void testInsert() throws Exception {
        Table table = utility.createTable(Bytes.toBytes("MyTest"),
                Bytes.toBytes("CF"));
        HBaseTestObj obj = new HBaseTestObj("testKey", "testValue");
        MyHBaseDAO.insertRecord(table, obj);
        Get get1 = new Get(Bytes.toBytes(obj.getRowKey()));
        get1.addColumn(CF, QUALIFIER);
        Result result1 = table.get(get1);
        assertEquals(Bytes.toString(result1.getRow()), obj.getRowKey());
        assertEquals(Bytes.toString(result1.value()), obj.getValue());
        
        LOG.log(Level.INFO, "RowKey: {0}", Bytes.toString(result1.getRow()));
        LOG.log(Level.INFO, "RowValue: {0}", Bytes.toString(result1.value()));
    }
    
    @After
    public void tearDown() {
        try {
            utility.shutdownMiniCluster();
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
    }
}
