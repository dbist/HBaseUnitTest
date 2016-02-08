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

import java.nio.ByteBuffer;
import java.util.logging.Logger;

/**
 *
 * @author aervits
 */
class HBaseTestObj {
    private static final Logger LOG = Logger.getLogger(HBaseTestObj.class.getName());
    
    private String rowKey;
    private String value;

    public HBaseTestObj(String rowKey, String value) {
        this.rowKey = rowKey;
        this.value = value;
    }

    String getRowKey() {
        return this.rowKey;
    }
    
    String getValue() {
        return this.value;
    }
}
