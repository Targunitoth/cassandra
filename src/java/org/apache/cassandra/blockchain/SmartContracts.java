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

import java.sql.Blob;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.UUID;

import javax.sql.rowset.serial.SerialBlob;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.exceptions.ContractParsingException;
import org.apache.cassandra.schema.TableMetadata;

public class SmartContracts
{
    String contractText = "";
    String user = "";
    String user1 = "";
    String user2 = "";
    int amount1 = -1;
    int amount2 = -1;
    boolean receive = true;
    int times = 0;
    //private int executeCounter = 0;
    private static HashMap<String, Integer> executionCounter = new HashMap<>();

    //Parse contract Text, example:
    //CONTRACT IF Alice RECEIVES 100 SEND 10 FROM Alice TO Bob
    //CONTRACT IF Bob PAYS 200 SEND 20 FROM Bob TO Alice ONLY 1
    public SmartContracts(String contractText)
    {
        if (contractText.isEmpty())
        {
            throw new ContractParsingException("Empty contract text");
        }
        String[] splitContract = contractText.split(" ");
        if (splitContract[0].toUpperCase().equals("CONTRACT"))
        {
            for (int i = 1; i < splitContract.length - 1; i += 2)
            {
                switch (splitContract[i].toUpperCase())
                {
                    case "IF":
                        user = splitContract[i + 1];
                        break;
                    case "RECEIVES":
                        receive = true;
                        amount1 = Integer.parseInt(splitContract[i + 1]);
                        break;
                    case "PAYS":
                        receive = false;
                        amount1 = Integer.parseInt(splitContract[i + 1]);
                        break;
                    case "SEND":
                        amount2 = Integer.parseInt(splitContract[i + 1]);
                        break;
                    case "FROM":
                        user1 = splitContract[i + 1];
                        break;
                    case "TO":
                        user2 = splitContract[i + 1];
                        break;
                    case "ONLY":
                        times = Integer.parseInt(splitContract[i + 1]);
                        break;
                    default:
                        throw new ContractParsingException("Keyword not found.");
                }
            }
        }
        else
        {
            throw new ContractParsingException("No CONTRACT keyword found.");
        }

        if (user.isEmpty())
        {
            throw new ContractParsingException("User not set");
        }
        if (user1.isEmpty())
        {
            user1 = user;
        }
        if (user2.isEmpty())
        {
            throw new ContractParsingException("User2 not set");
        }
        if (amount1 == -1)
        {
            throw new ContractParsingException("Amount1 not set");
        }
        if (amount2 == -1)
        {
            throw new ContractParsingException("Amount2 not set");
        }
		if (amount2 > amount1)
        {
            throw new ContractParsingException("Amount2 is bigger then Amount1. This could lead to a endless loop");
        }
        this.contractText = contractText;
    }

    //Check if the Contract must be executed
    public boolean checkContract(String source, String dest, int amount)
    {
        if (amount >= amount1)
        {
            if (receive)
            {
                if (dest.equals(user))
                {
                    return true;
                }
            }
            else
            {
                if (source.equals(user))
                {
                    return true;
                }
            }
        }
        return false;
    }

    public int getNumberOfExecutions()
    {
        if (executionCounter.containsKey(contractText))
        {
            return executionCounter.get(contractText);
        }
        else
        {
            return 0;
        }
    }

    @Override
    public String toString()
    {
        return contractText;
    }

    public void executeContract(TableMetadata metadata)
    {
        boolean debug = BlockchainHandler.getDebug();
        //Only execute X times
        if (times == 0 || getNumberOfExecutions() < times)
        {
            String query = "INSERT INTO ";
            query += metadata.keyspace + "." + metadata.name;
            if (debug)
            {
                query += " (blockchainid, destination, amount) VALUES (" + UUIDs.timeBased() + ", ";
                query += " '" + user2 + "', " + amount2 + ");";
            }
            else
            {
                query += " (blockchainid, destination, amount, timestamp, predecessor, hash) VALUES (" + UUIDs.timeBased() + ", ";
                query += " '" + user2 + "', " + amount2 + ", null, null, null);";
            }

            FormatHelper.executeQuery(query);
            executionCounter.put(contractText, getNumberOfExecutions() + 1);
        }
    }
}
