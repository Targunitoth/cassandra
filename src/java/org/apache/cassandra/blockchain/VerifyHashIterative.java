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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.BlockchainBrokenException;
import org.apache.cassandra.schema.ColumnMetadata;

/***
 * Generate a routine to verify if the HashChain is unbroken
 */
public class VerifyHashIterative extends VerifyHash
{
    //List with key, callable with sting column names
    private static HashMap<ByteBuffer, HashMap<String, ByteBuffer>> table;

    public static boolean verify(String tableName)
    {

        System.out.println("Start validating TABLE " + tableName);
        setTableName(tableName);
        loadMetadata();
        generateTable();

        //Validate, start with head
        String calculatedHash = validateList();
        return calculatedHash.equals(HashBlock.getPredecessorHash());
    }

    private static void generateTable()
    {
        table = new HashMap<>();
        Object[] obj = new Object[0];
        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM " + tableName, obj);
        String columname;

        for (UntypedResultSet.Row row : rs)
        {
            row.printFormatet();
        }

        //Get the first Key column name
        ImmutableList<ColumnMetadata> columnMetadata = metadata.partitionKeyColumns();
        ListIterator<ColumnMetadata> columnMetadataListIterator = columnMetadata.listIterator();
        String keyName = columnMetadataListIterator.next().name.toString();

        for (UntypedResultSet.Row row : rs)
        {
            List<ColumnSpecification> cs = row.getColumns();

            HashMap<String, ByteBuffer> tmp = new HashMap<>();
            ByteBuffer key = null;
            for (ColumnSpecification column : cs)
            {
                columname = column.name.toString();
                if (columname.contains(keyName))
                {
                    key = row.getBytes(columname);
                }
                else
                {
                    tmp.put(columname, row.getBytes(columname));
                }
            }
            table.put(key, tmp);
        }
    }

    private static String validateList()
    {
        List<ByteBuffer> order = new LinkedList<>();
        //Set first key to head of blockchain
        ByteBuffer key = HashBlock.getBlockChainHead();
        //Sort List
        do
        {
            order.add(0, key);
            key = table.get(key).get("predecessor");
        } while (!key.equals(HashBlock.getNullBlock()));

        System.out.println("Ordered");
        for (ByteBuffer orderedkey : order)
        {
            System.out.println("Key: " + FormatHelper.convertByteBufferToString(orderedkey));
        }


        ByteBuffer[] valueColumns = new ByteBuffer[metadata.columns().size()];
        ByteBuffer timestamp = null;

        String hash = "";
        String lastHash = "";
        String calculatedHash;

        int cvcounter;

        for (ByteBuffer orderedkey : order)
        {
            cvcounter = 0;

            for (ColumnMetadata cm : metadata.columns())
            {
                String cmname = cm.name.toString();
                if (cmname.equals(HashBlock.getBlockchainIDString()))
                {
                    //Do nothing for the key Value
                }
                else if (cmname.equals("timestamp"))
                {
                    timestamp = table.get(orderedkey).get(cm.name.toString());
                }
                else if (cmname.equals("hash"))
                {
                    lastHash = UTF8Type.instance.compose(table.get(orderedkey).get(cm.name.toString()));
                }
                else
                {
                    valueColumns[cvcounter++] = table.get(orderedkey).get(cm.name.toString());
                }
            }
            calculatedHash = HashBlock.calculateHash(orderedkey, removeEmptyCells(valueColumns), timestamp, hash);
            //System.out.println(calculatedHash);
            if(!calculatedHash.equals(lastHash)){
                throw new BlockchainBrokenException(orderedkey, calculatedHash);
            }
            hash = lastHash;
        }

        return hash;
    }
}
