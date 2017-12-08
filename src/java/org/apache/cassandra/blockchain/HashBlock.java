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
import java.util.UUID;

import com.sun.jersey.api.NotFoundException;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.UTF8Type;

public class HashBlock
{
    public final static String tables[] = { "blockchainid", "predecessor", "hash" };
    public final static CQL3Type.Native types[] = { CQL3Type.Native.TIMEUUID, CQL3Type.Native.TIMEUUID, CQL3Type.Native.TEXT };
    public static ColumnIdentifier identifier[];


    private static final UUID nullBlock = UUID.fromString("00000000-0000-0000-0000-000000000000");
    private static UUID blockChainHead = null;
    private static boolean writeableHead = false; //Zuerst muss einmal gelesen werden, bevor gesetzt werden kann.
    private static String predecessorHash = "";


    public static java.util.UUID getBlockChainHead()
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

    public static void setBlockChainHead(java.util.UUID newHead)
    {
        //Nach jedem Schreiben des Headers muss er erst wieder gelesen werden
        blockChainHead = newHead;
        //TODO Optional anounce new Head with gossip
    }

    public static ByteBuffer generateHash(UUID key, String[] cellValues, long timestamp)//insert Cell
    {

        String delimiter = "|";
        String input = key.toString();
        input += delimiter + "" + timestamp;

        for (String item : cellValues)
        {
            input += delimiter + "" + item;
        }

        input += delimiter + "" + predecessorHash;

        System.out.println("Hash calculation for: " + input);

        String sha256hex = org.apache.commons.codec.digest.DigestUtils.sha256Hex(input);

        //Set this key and hash as new predecessor
        HashBlock.setBlockChainHead(key);
        predecessorHash = sha256hex;

        return UTF8Type.instance.decompose(sha256hex);
    }

    public static String getBlockchainIDString()
    {
        return tables[0];
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

    public static UUID getNullBlock()
    {
        return nullBlock;
    }
}
