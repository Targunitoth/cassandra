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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.interfaces.DSAParams;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.utils.Pair;
import sun.security.provider.DSAPrivateKey;
import sun.security.provider.DSAPublicKey;


public class DigitalSignature
{
    HashMap<String, Pair<PrivateKey, PublicKey>> lokalKeyStore = new HashMap<>();
    boolean createSignature = false;
    String createSignatureName = "";

    public DigitalSignature()
    {
        //TODO Remove This, for testing we need Keys for Bob and Alice
        genKey("alice");
        genKey("bob");

        //TODO Remove this
        //printKeyDatabase();
    }

    private void printKeyDatabase()
    {
        System.out.println("Print Database for debugging:");
        for (Map.Entry<String, Pair<PrivateKey, PublicKey>> entry : lokalKeyStore.entrySet())
        {
            System.out.println("Name: " + entry.getKey() + " - Keypair: " + entry.getValue().toString());
        }
    }

    private void genKey(String name)
    {
        try
        {
            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("DSA", "SUN");

            SecureRandom random = SecureRandom.getInstance("SHA1PRNG", "SUN");
            keyGen.initialize(1024, random);

            KeyPair pair = keyGen.generateKeyPair();
            PrivateKey priv = pair.getPrivate();
            PublicKey pub = pair.getPublic();

            saveKey(name.toLowerCase(), pub, priv);

            loadKeysFromDatabase(name.toLowerCase());
        }
        catch (Exception e)
        {
            System.err.println("Caught exception " + e.toString());
        }
    }

    private void loadKeysFromDatabase(String name)
    {
        PublicKey pub = loadPubKeyFromDatabase(name);
        PrivateKey priv = loadPrivKeyFromDatabase(name);

        lokalKeyStore.put(name, Pair.create(priv, pub));
    }

    private PrivateKey loadPrivKeyFromDatabase(String name)
    {
        UntypedResultSet urs = FormatHelper.executeQuery("SELECT p,q,g,x FROM blockchain.keydatabase WHERE user = '" + name.toLowerCase() + "'");
        UntypedResultSet.Row row = urs.one();
        BigInteger prime = row.getVarint("p");
        BigInteger subPrime = row.getVarint("q");
        BigInteger base = row.getVarint("g");
        BigInteger x = row.getVarint("x");

        try
        {
            return new DSAPrivateKey(x, prime, subPrime, base);
        }
        catch (Exception e)
        {
            System.err.println("Caught exception " + e.toString());
        }
        return null;
    }

    private PublicKey loadPubKeyFromDatabase(String name)
    {
        UntypedResultSet urs = FormatHelper.executeQuery("SELECT p,q,g,y FROM blockchain.keydatabase WHERE user = '" + name.toLowerCase() + "'");
        if(urs.isEmpty()){
            //Key not loadable, generate it
            genKey(name.toLowerCase());
            return getPublicKey(name.toLowerCase());
        }
        UntypedResultSet.Row row = urs.one();
        BigInteger prime = row.getVarint("p");
        BigInteger subPrime = row.getVarint("q");
        BigInteger base = row.getVarint("g");
        BigInteger y = row.getVarint("y");

        try
        {
            return new DSAPublicKey(y, prime, subPrime, base);
        }
        catch (Exception e)
        {
            System.err.println("Caught exception " + e.toString());
        }
        return null;
    }

    private void saveKey(String name, PublicKey pub, PrivateKey priv)
    {
        DSAPrivateKey privateKey = (DSAPrivateKey) priv;
        DSAPublicKey publicKey = (DSAPublicKey) pub;
        /* DSA requires three parameters to create a key pair
         *  prime (P)
         *  subprime (Q)
         *  base (G)
         * These three values are used to create a private key (X)
         * and a public key (Y)
         */
        DSAParams dsaParams = privateKey.getParams();
        BigInteger prime = dsaParams.getP();
        BigInteger subPrime = dsaParams.getQ();
        BigInteger base = dsaParams.getG();
        BigInteger x = privateKey.getX();
        BigInteger y = publicKey.getY();

        FormatHelper.executeQuery("INSERT INTO blockchain.keydatabase (user, p, q, g, x, y) VALUES ('" + name.toLowerCase() + "', " + prime + ", " + subPrime + ", " + base + ", " + x + ", " + y + ") IF NOT EXISTS;");
    }


    private boolean checkFalseName(String name)
    {
        if(name == null || name.equals("")){
            return true;
        }
        if(!lokalKeyStore.containsKey(name.toLowerCase())){
            loadKeysFromDatabase(name.toLowerCase());
        }
        return false;
    }

    public ByteBuffer createSignature(ByteBuffer source, ByteBuffer dest, ByteBuffer amount, ByteBuffer timestamp){
        if(createSignature == false)
            return null;

        //toggle to false, only create signature if wished
        createSignature = false;

        if(source == null){
            return null;
        }
        return ByteBuffer.wrap(signData(createSignatureName, source, dest, amount, timestamp));
    }

    public byte[] signData(String name, ByteBuffer... values)
    {
        if (checkFalseName(name))
        {
            return null;
        }
        try
        {
            Signature dsa = Signature.getInstance("SHA1withDSA", "SUN");
            dsa.initSign(lokalKeyStore.get(name.toLowerCase()).left);

            dsa.update(convertBytebufferArrayToByte(values));
            //make a bytebuffer for easy saving
            return dsa.sign();
        }
        catch (Exception e)
        {
            System.err.println("Caught exception " + e.toString());
        }
        return null;
    }

    private byte[] convertBytebufferArrayToByte(ByteBuffer[] values)
    {
        //Remove null values before sort
        values = FormatHelper.removeNull(values);

        //Sort Array to geht always the same
        Arrays.sort(values);

        //create a new temporary buffer
        byte[] tmpbuffer = new byte[1024 * 1024];
        int counter = 0;

        //Create one big buffer from all bytes
        for (ByteBuffer value : values)
        {
            for (int i = value.position(); i < value.limit(); i++)
            {
                tmpbuffer[counter++] = value.get(i);
            }
        }

        //remove the null values
        byte[] buffer = new byte[counter];
        for (int i = 0; i < counter; i++)
        {
            buffer[i] = tmpbuffer[i];
        }

        return buffer;
    }

    public boolean verifyData(String name, byte[] signature, ByteBuffer... values)
    {
        if (checkFalseName(name))
        {
            return false;
        }
        try
        {
            Signature dsa = Signature.getInstance("SHA1withDSA", "SUN");

            dsa.initVerify(getPublicKey(name.toLowerCase()));

            dsa.update(convertBytebufferArrayToByte(values));

            return dsa.verify(signature);
        }
        catch (Exception e)
        {
            System.err.println("Caught exception " + e.toString());
        }
        return false;
    }

    public PublicKey getPublicKey(String name)
    {
        if (checkFalseName(name))
        {
            return null;
        }
        return lokalKeyStore.get(name.toLowerCase()).right;
    }

    public void triggerCreateSignature(String name)
    {
        createSignature = true;
        createSignatureName = name;
    }

    public static void vaidateTable(String tableName)
    {
        //TODO Check Signature for every entry in the Table
    }
}
