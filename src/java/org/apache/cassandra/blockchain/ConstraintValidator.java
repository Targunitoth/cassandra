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

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.NegativeAmountException;
import org.apache.cassandra.exceptions.NoSignatureException;
import org.apache.cassandra.exceptions.SignatureValidationFailedException;
import org.apache.cassandra.exceptions.SpendTooMuchException;
import org.apache.cassandra.schema.TableMetadata;

public class ConstraintValidator
{
    ByteBuffer amount;
    ByteBuffer source;
    TableMetadata metadata;

    //TODO Create a List of all possible entries, with Key, Pre, amount, sorce, dest ===> Lock at iterate hash
    //TODO Oder nur Key + pre und dann select???

    public ConstraintValidator(ByteBuffer amount, ByteBuffer source, TableMetadata metadata)
    {
        this.amount = amount;
        this.source = source;
        this.metadata = metadata;
    }

    public boolean validateMoney()
    {
        //Amount must be positiv
        Integer money = Int32Type.instance.compose(amount);
        if(money < 0){
            //Throw Exception for debugging
            throw new NegativeAmountException(money);
            //return false;
        }

        //Bring money into the system
        if(source == null){
            return true;
        }

        //TODO Calc source money only from Blockchain
        UntypedResultSet rs = FormatHelper.executeQuery("SELECT sum(amount) FROM " + metadata.keyspace + "." + metadata.name + " WHERE destination = '" + UTF8Type.instance.compose(source) + "' ALLOW FILTERING;");
        Integer balance = rs.one().getInt("system.sum(amount)");

        rs = FormatHelper.executeQuery("SELECT sum(amount) FROM " + metadata.keyspace + "." + metadata.name + " WHERE source = '" + UTF8Type.instance.compose(source) + "' ALLOW FILTERING;");
        balance -= rs.one().getInt("system.sum(amount)");

        if(balance < money){
            //Throw Exception for debugging
            throw new SpendTooMuchException(UTF8Type.instance.compose(source), money, balance);
            //return false;
        }

        return true;
    }

    public void validateSignature(ByteBuffer dest, ByteBuffer sig, ByteBuffer time)
    {
        if(source == null)
        {
            //signature useless
            return;
        }
        if(sig == null)
        {
            throw new NoSignatureException(UTF8Type.instance.compose(source));
        }
        if(!HashBlock.getDs().verifyData(UTF8Type.instance.compose(source), sig.array(), source, dest, amount, time)){
            throw new SignatureValidationFailedException(UTF8Type.instance.compose(source), source);
        }
    }

}
