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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.schema.TableMetadata;

public class BlockchainHandler
{
    public final static String tables[] = { "blockchainid", "signature", "timestamp", "predecessor", "hash" };
    public final static CQL3Type.Native types[] = { CQL3Type.Native.TIMEUUID, CQL3Type.Native.BLOB, CQL3Type.Native.TIMESTAMP, CQL3Type.Native.TIMEUUID, CQL3Type.Native.TEXT };
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
    private static DigitalSignature ds;
    private static TreeNode blocktree = null;

    private static ArrayList<SmartContracts> smartContracts = null;
    private static ArrayList<String> executeableContracts = null;


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
        initDS();
        initSC();
    }

    private static void initSC()
    {
        if (smartContracts == null)
        {
            smartContracts = new ArrayList<>();
        }
        if (executeableContracts == null)
        {
            executeableContracts = new ArrayList<>();
        }

        //TODO Restore all SmartContracts
    }

    public static void initDS()
    {
        if (ds == null)
        {
            ds = new DigitalSignature();
        }
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

    public static ArrayList<SmartContracts> getSmartContracts()
    {
        if (!init)
        {
            init();
        }
        return smartContracts;
    }

    public static void addSmartContracts(SmartContracts sc)
    {
        if (!init)
        {
            init();
        }
        smartContracts.add(sc);
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
        BlockchainHandler.setBlockChainHead(key);

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

        //Remove null values
        cellValues = FormatHelper.removeNull(cellValues);

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

    public static void createBlockchainTables()
    {
        blockChainHead = null;
        predecessorHash = "";
        init = true;
        FormatHelper.executeQuery("CREATE KEYSPACE IF NOT EXISTS blockchain WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};");
        FormatHelper.executeQuery("CREATE TABLE IF NOT EXISTS blockchain.blockchainheader (nullblock UUID PRIMARY KEY, hash TEXT, predecessor UUID);");
        FormatHelper.executeQuery("CREATE TABLE IF NOT EXISTS blockchain.keydatabase (user TEXT PRIMARY KEY, p VARINT, q VARINT, g VARINT, x VARINT, y VARINT);");

        saveNewHead();

        ds = new DigitalSignature();
        blocktree = new TreeNode(nullBlock);
        smartContracts = new ArrayList<>();
        executeableContracts = new ArrayList<>();
    }

    public static void setDebug(boolean debug)
    {
        BlockchainHandler.debug = debug;
    }

    public static boolean getDebug()
    {
        return debug;
    }

    public static DigitalSignature getDs()
    {
        return ds;
    }


    public static TreeNode getBlocktree(TableMetadata metadata)
    {
        if (blocktree == null)
        {
            TreeNode.updatMetadata(metadata);
            blocktree = TreeNode.buildTree();
        }
        return blocktree;
    }

    public static void addExecutableContractStrings(String contract)
    {
        if (!init)
        {
            init();
        }
        executeableContracts.add(contract);
    }

    public static ArrayList<String> getExecutableContracts()
    {
        if (!init)
        {
            init();
        }
        return executeableContracts;
    }
}
