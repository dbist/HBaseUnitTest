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

/**
 *
 * @author aervits
 */
public class MyHBaseDAO {
    private static final Logger LOG = Logger.getLogger(MyHBaseDAO.class.getName());
    
    
    public static void main (String [] args) {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("zookeeper.znode.parent", "/hbase-unsecure");
            
            Connection connection = ConnectionFactory.createConnection(conf);
            
            Table table = connection.getTable(TableName.valueOf("tableName"));
            HBaseTestObj obj = new HBaseTestObj("rowKey1", "myValue");
            
            insertRecord(table, obj);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
    }
    
    public static void insertRecord(Table table, HBaseTestObj obj) throws Exception {
        Put put = createPut(obj);
        table.put(put);
    }
    
    public static Put createPut(HBaseTestObj obj) {
        Put put = new Put(Bytes.toBytes(obj.getRowKey()));
        put.addColumn(Bytes.toBytes("CF"), Bytes.toBytes("CQ-1"),
                    Bytes.toBytes(obj.getValue()));
        return put;
    }
}

