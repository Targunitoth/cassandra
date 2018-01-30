/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.blockchain;

import java.nio.ByteBuffer;

import javax.xml.bind.SchemaOutputResolver;

import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

public abstract class VerifyHash
{
    //Global Variables
    protected static TableMetadata metadata;
    protected static String tableName;

    protected static void setTableName(String tblName)
    {
        tableName = tblName;
    }

    protected static void loadMetadata()
    {
        assert tableName != "": "Table name must be set!";
        if(tableName.contains("."))
        {
            String[] tableNameSplit = tableName.split("\\.");
            metadata = Schema.instance.validateTable(tableNameSplit[0], tableNameSplit[1]);
        }else {
            System.out.println("Please provide a keyspace. Trying default: mykeyspace.");
            metadata = Schema.instance.validateTable("mykeyspace", tableName);
        }
    }

    protected static ByteBuffer[] removeEmptyCells(ByteBuffer[] array)
    {
        ByteBuffer[] result;
        int counter = 0;
        boolean shrinkSize = false;
        for (ByteBuffer bb : array)
        {
            if (bb != null)
            {
                counter++;
            }
            else
            {
                shrinkSize = true;
            }
        }
        if (shrinkSize)
        {
            result = new ByteBuffer[counter];
            counter = 0;
            for (ByteBuffer bb : array)
            {
                if (bb != null)
                {
                    result[counter++] = bb;
                }
            }
            return result;
        }
        else
        {
            return array;
        }
    }
}
