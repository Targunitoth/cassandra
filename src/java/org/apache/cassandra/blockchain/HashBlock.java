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
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import com.sun.jersey.api.NotFoundException;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;

public class HashBlock
{
    public final static String tables[] = { "blockchainid", "predecessor", "hash", "timestamp" };
    public final static CQL3Type.Native types[] = { CQL3Type.Native.TIMEUUID, CQL3Type.Native.TIMEUUID, CQL3Type.Native.TEXT, CQL3Type.Native.TIMESTAMP };
    public static ColumnIdentifier identifier[];

    //Set nullBlock to ones for debugging
    private static final ByteBuffer nullBlock = UUIDType.instance.decompose(UUID.fromString("00000000-0000-0000-0000-000000000000"));
    //private static final ByteBuffer nullBlock = UUIDType.instance.decompose(UUID.fromString("11111111-1111-1111-1111-111111111111"));
    private static ByteBuffer blockChainHead = null;

    private static String predecessorHash = "";

    private static String delimiter = "|";


    public static ByteBuffer getBlockChainHead()
    {
        //Am anfang ist der header leer, dann gib 0 zurück
        if (blockChainHead == null)
        {
            return nullBlock;
        }
        else
        {
            return blockChainHead;
        }
    }

    private static void setBlockChainHead(ByteBuffer newHead)
    {
        //Nach jedem Schreiben des Headers muss er erst wieder gelesen werden
        blockChainHead = newHead;
        //TODO Optional anounce new Head with gossip
    }

    /***
     * Generates the Hash for the Cells
     * @param key Key of the Row
     * @param cellValues Value of the cells of the row
     * @param timestamp current timestamp
     * @return
     */
    public static ByteBuffer generateAndSetHash(ByteBuffer key, ByteBuffer[] cellValues, ByteBuffer timestamp)//insert Cell
    {
        //Calculate hash
        String sha256hex = calculateHash(key, cellValues, timestamp, predecessorHash);

        //Set this key and hash as new predecessor
        HashBlock.setBlockChainHead(key);
        predecessorHash = sha256hex;

        //Convert the String to ByteBuffer and return it
        return UTF8Type.instance.decompose(sha256hex);
    }

    public static String calculateHash(ByteBuffer key, ByteBuffer[] cellValues, ByteBuffer timestamp, String preHash)
    {
        //Set key as first input
        String input = FormatHelper.convertByteBufferToString(key);

        //Add timestamp
        input += delimiter + "" + FormatHelper.convertByteBufferToString(timestamp);

        //Sort the array to make sure to always geht the same order before hashing
        Arrays.sort(cellValues);
        //Add the value of the cells
        for (ByteBuffer value : cellValues)
        {
            input += delimiter + "" + FormatHelper.convertByteBufferToString(value);
        }

        //Add the predecessor Hash
        input += delimiter + "" + preHash;

        //For Testing: Break Blockchain, by adding random values
        //Random rand = new Random();
        //input += delimiter + "" + rand.nextInt(10);

        //For Debuging
        //System.out.println("Hash calculation for: " + input);

        //Generate SHA256 Hash
        return org.apache.commons.codec.digest.DigestUtils.sha256Hex(input);
    }

    public static String getBlockchainIDString()
    {
        return tables[0];
    }

    public static String getTableName(int number)
    {
        if (number < 0 || number >= tables.length)
        {
            throw new IndexOutOfBoundsException();
        }
        return tables[number];
    }

    public static ColumnIdentifier getIdentifer(String tablename) throws NotFoundException
    {
        int index = -1;
        for (int i = 0; i < tables.length; i++)
        {
            if (tables[i].equals(tablename))
            {
                index = i;
                break;
            }
        }
        if (index < 0 || index >= tables.length)
        {
            throw new NotFoundException();
        }
        return identifier[index];
    }

    public static ByteBuffer getNullBlock()
    {
        return nullBlock;
    }

    public static String getPredecessorHash()
    {
        return predecessorHash;
    }
}
