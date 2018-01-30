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

import com.datastax.driver.core.utils.UUIDs;
import com.sun.jersey.api.NotFoundException;
import io.netty.channel.ChannelOutboundBuffer;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.index.sasi.disk.OnDiskIndex;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.Message;

public class HashBlock
{
    public final static String tables[] = { "blockchainid", "predecessor", "hash", "timestamp" };
    public final static CQL3Type.Native types[] = { CQL3Type.Native.TIMEUUID, CQL3Type.Native.TIMEUUID, CQL3Type.Native.TEXT, CQL3Type.Native.TIMESTAMP };
    public static ColumnIdentifier identifier[];

    //Set nullBlock to ones for debugging
    //private static final ByteBuffer nullBlock = UUIDType.instance.decompose(org.apache.cassandra.utils.LOW));
    private static final ByteBuffer nullBlock = UUIDType.instance.decompose(UUID.fromString("00000000-0000-1000-8080-808080808080"));

    private static ByteBuffer blockChainHead = null;
    private static String predecessorHash = "";

    //TODO Can be empty string, only for debugging relevant
    private static String delimiter = "|";

    private static boolean init = false;
    private static boolean debug = false;


    public static ByteBuffer getBlockChainHead()
    {
        //Am anfang ist der header leer, dann gib 0 zurück
        if (blockChainHead == null)
        {
            if (!init)
            {
                init();
            }
            return nullBlock;
        }
        else
        {
            return blockChainHead;
        }
    }

    private static void init()
    {
        init = true;
        restoreData();
    }

    private static void restoreData()
    {
        UntypedResultSet rs;
        if (!debug)
        {
            rs = QueryProcessor.execute("SELECT hash, predecessor FROM blockchain.blockchainheader WHERE nullblock = ?;", ConsistencyLevel.ONE, getNullBlock());
        }
        else
        {
            rs = QueryProcessor.executeInternal("SELECT hash, predecessor FROM blockchain.blockchainheader WHERE nullblock = ?;", getNullBlock());
        }
        predecessorHash = rs.one().getString("hash");
        blockChainHead = rs.one().getBytes("predecessor");
    }

    private static void setBlockChainHead(ByteBuffer newHead)
    {
        //Nach jedem Schreiben des Headers muss er erst wieder gelesen werden
        blockChainHead = newHead;
        //Save in database
        saveNewHead();
    }

    private static void saveNewHead()
    {
        if (!debug)
        {
            QueryProcessor.execute("INSERT INTO blockchain.blockchainheader (nullblock, hash, predecessor) values (?, ?, ?);", ConsistencyLevel.ONE, getNullBlock(), getPredecessorHash(), getBlockChainHead());
        }
        else
        {
            QueryProcessor.executeInternal("INSERT INTO blockchain.blockchainheader (nullblock, hash, predecessor) values (?, ?, ?);", getNullBlock(), getPredecessorHash(), getBlockChainHead());
        }
    }

    /***
     * Generates the Hash for the Cells
     * @param key Key of the Row
     * @param cellValues Value of the cells of the row
     * @param timestamp Timestamp
     * @return
     */
    public static ByteBuffer generateAndSetHash(ByteBuffer key, ByteBuffer[] cellValues, ByteBuffer timestamp)//insert Cell
    {
        //Calculate hash
        String sha256hex = calculateHash(key, cellValues, timestamp, predecessorHash);

        //Set this key and hash as new predecessor
        predecessorHash = sha256hex;
        HashBlock.setBlockChainHead(key);

        //For Debuging
        System.out.println("Calculatet Hash: " + sha256hex);

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
        System.out.println("Hash calculation for: " + input);

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

    public static ColumnIdentifier getIdentifer(String tablename)
    {
        if (!init)
        {
            init();
        }
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
            //TODO throw new IndexOutOfBoundsException or not found
            return null;
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

    public static void createHeaderTable()
    {
        blockChainHead = null;
        predecessorHash = "";
        init = true;
        if (!debug)
        {
            QueryProcessor.execute("CREATE KEYSPACE IF NOT EXISTS blockchain WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};", ConsistencyLevel.ONE);
        }
        else
        {
            QueryProcessor.executeInternal("CREATE KEYSPACE IF NOT EXISTS blockchain WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2};");
        }
        if (!debug)
        {
            QueryProcessor.execute("CREATE TABLE blockchain.blockchainheader (nullblock uuid primary key, hash text, predecessor uuid);", ConsistencyLevel.ONE);
        }
        else
        {
            QueryProcessor.executeInternal("create table blockchain.blockchainheader (nullblock uuid primary key, hash text, predecessor uuid);");
        }

        saveNewHead();
    }

    public static void setDebug(boolean debug)
    {
        HashBlock.debug = debug;
    }
}
