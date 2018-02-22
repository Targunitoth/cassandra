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

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.exceptions.BlockchainBrokenException;
import org.apache.cassandra.schema.ColumnMetadata;


/***
 * Generate a routine to verify if the HashChain is unbroken
 */
public class VerifyHashRecursive extends VerifyHash
{
    public static boolean verify(String tableName)
    {
        //Preparation
        setTableName(tableName);
        loadMetadata();

        //Recursive Call
        System.out.println("Start validating table " + tableName + " recursively");
        String calculatedHash = validateRecursive(BlockchainHandler.getBlockChainHead());
        //Test Value
        return calculatedHash.equals(BlockchainHandler.getPredecessorHash());
    }

    private static String validateRecursive(ByteBuffer key)
    {
        if (key.equals(BlockchainHandler.getNullBlock()))
        {
            return "";
        }

        String cmname;
        ByteBuffer[] valueColumns = new ByteBuffer[metadata.columns().size() - 3];
        ByteBuffer timestamp = null;
        int counter = 0;
        String calculatedHash;
        String thisHash = "";

        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM " + tableName + " WHERE " + BlockchainHandler.getBlockchainIDString() + "=?", key);
        if (!rs.isEmpty())
        {
            UntypedResultSet.Row row = rs.one();
            for (ColumnMetadata cm : metadata.columns())
            {
                cmname = cm.name.toString();

                if (cmname.equals(BlockchainHandler.getBlockchainIDString()))
                {
                    //Do nothing for the key, it is already set
                }
                else if (cmname.equals("timestamp"))
                {
                    timestamp = row.getBytes("timestamp");
                }
                else if (cmname.equals("hash"))
                {
                    //for debugging
                    System.out.println("Selected Row:");
                    row.printFormated();

                    assert row.has("hash") : "Row is not empty and does not contain hast. Maybe table is broken... ";

                    //Save the current hash to return it at the end
                    thisHash = row.getString("hash");
                }
                else
                {
                    valueColumns[counter++] = row.getBytes(cmname);
                }
            }
            calculatedHash = BlockchainHandler.calculateHash(key, removeEmptyCells(valueColumns), timestamp, validateRecursive(row.getBytes("predecessor")));
            if (!calculatedHash.equals(thisHash))
            {
                throw new BlockchainBrokenException(key, calculatedHash);
            }
        }
        return thisHash;
    }
}
