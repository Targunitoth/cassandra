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
import java.util.LinkedList;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.NegativeAmountException;
import org.apache.cassandra.exceptions.NoSignatureException;
import org.apache.cassandra.exceptions.SignatureValidationFailedException;
import org.apache.cassandra.exceptions.SpendTooMuchException;
import org.apache.cassandra.schema.TableMetadata;

public class ConstraintValidator
{
    ByteBuffer keyBuffer;
    ByteBuffer amountBuffer;
    ByteBuffer sourceBuffer;
    TableMetadata metadata;
    int amount = 0;
    String source = "";

    //TODO Create a List of all possible entries, with Key, Pre, amount, sorce, dest ===> Lock at iterate hash
    //TODO Oder nur Key + pre und dann select???

    public ConstraintValidator(ByteBuffer key, ByteBuffer amount, ByteBuffer source, TableMetadata metadata)
    {
        assert amount != null : "Value can not be null";
        this.amountBuffer = amount;
        this.sourceBuffer = source;
        this.metadata = metadata;
        this.keyBuffer = key;
        if (source != null)
        {
            this.source = UTF8Type.instance.compose(source);
        }
        if (amount != null)
        {
            this.amount = Int32Type.instance.compose(amount);
        }
    }

    public boolean validateMoney()
    {
        if (amount < 0)
        {
            //Throw Exception for debugging
            throw new NegativeAmountException(amount);
            //return false;
        }

        //Bring money into the system
        if (sourceBuffer == null)
        {
            return true;
        }

        UntypedResultSet rs;
        Integer balance = 0;

/*
        //Calc source money only from Blockchain (Easy Mode)
        rs = FormatHelper.executeQuery("SELECT sum(amount) FROM " + metadata.keyspace + "." + metadata.name + " WHERE destination = '" + source + "' ALLOW FILTERING;");
        balance = rs.one().getInt("system.sum(amount)");

        rs = FormatHelper.executeQuery("SELECT sum(amount) FROM " + metadata.keyspace + "." + metadata.name + " WHERE source = '" + source + "' ALLOW FILTERING;");
        balance -= rs.one().getInt("system.sum(amount)");
*/


        //Calculate money with HashTree
        TreeNode blockchainTree = BlockchainHandler.getBlocktree(metadata);
        System.out.println(blockchainTree.printTreeLeafs());
        LinkedList<TreeNode> lp = blockchainTree.getLongestPath();
        //Debug
        //System.out.println("Number of deepest Paths: " + lp.size());
        assert lp.size() >= 1 : "Something went wrong! BlockchainTree longest path had no result";

        //Only use the first element for calculating
        TreeNode deepestLeaf = lp.getFirst();
        //No money if there is no tree
        if (deepestLeaf.isRoot())
        {
            return true;
        }

        //Create a pointer through the tree
        TreeNode treePointer = deepestLeaf;

        ByteBuffer s;
        ByteBuffer d;
        do
        {


            rs = FormatHelper.executeQuery("SELECT amount, source, destination FROM " + metadata.keyspace + "." + metadata.name + " WHERE blockchainid = " + TimeUUIDType.instance.compose(treePointer.data).toString() + ";");
            UntypedResultSet.Row row = rs.one();
            row.printFormated();
            s = row.getBytes("source");
            if (s != null && s.equals(sourceBuffer))
            {
                balance -= row.getInt("amount");
            }
            d = row.getBytes("destination");
            if (d != null && d.equals(sourceBuffer))
            {
                balance += row.getInt("amount");
            }
            treePointer = treePointer.parent;
        } while (!treePointer.isRoot());


        //Check money
        if (balance < amount)
        {
            //Throw Exception for debugging
            throw new SpendTooMuchException(source, amount, balance);
            //return false;
        }

        return true;
    }

    public void validateSignature(ByteBuffer dest, ByteBuffer sig, ByteBuffer time)
    {
        if (sourceBuffer == null)
        {
            //signature no neccessary
            return;
        }
        if (sig == null)
        {
            System.out.println("Source: " + source);
            System.out.println("Destination: " + UTF8Type.instance.compose(dest));
            System.out.println("Amount: " + amount);
            System.out.println("Sig: " + FormatHelper.convertByteBufferToString(sig));
            throw new NoSignatureException(source);
        }
        if (!BlockchainHandler.getDs().verifyData(source, sig.array(), sourceBuffer, dest, amountBuffer, time))
        {
            throw new SignatureValidationFailedException(source, sourceBuffer);
        }
    }

    public SmartContracts checkSmartContracts(ByteBuffer destBuffer)
    {
        if (!BlockchainHandler.getSmartContracts().isEmpty())
        {
            for (SmartContracts sc : BlockchainHandler.getSmartContracts())
            {
                //TODO Check more then one maybe
                if (sc.checkContract(source, UTF8Type.instance.compose(destBuffer), amount) == true)
                {
                    return sc;
                }
            }
        }
        return null;
    }
}
