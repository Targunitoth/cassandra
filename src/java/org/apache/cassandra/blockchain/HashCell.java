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

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;

import org.apache.commons.lang3.StringUtils;

public class HashCell
{
    ColumnMetadata column;
    long timestamp;
    ByteBuffer value;
    String sha256hex;

    public HashCell(ColumnMetadata column, long timestamp, ByteBuffer value)
    {
        System.out.println("HashCell Called!");

        this.column = column;
        this.timestamp = timestamp;
        this.value = value;
        String valueString = new String(value.array());

        //If there are non-printable characters, print the value in hex format
        if (!StringUtils.isAsciiPrintable(valueString))
        {
            valueString = asHex(value.array());
        }

        System.out.println("Timestamp: " + timestamp + "\tColumn: " + column + "\tValue: " + valueString);

        //TODO Add last block to SHA256
        //Generate SHA256
        sha256hex = org.apache.commons.codec.digest.DigestUtils.sha256Hex(column + "" + valueString + "" + timestamp);
        System.out.println("SHA256: " + sha256hex);
    }

    ///Helper variable for asHex
    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    /***
     * Transform a buffer into a hex string
     * @param buf Hex Buffer
     * @return String as HexValue
     */
    public static String asHex(byte[] buf)
    {
        char[] chars = new char[2 * buf.length];
        for (int i = 0; i < buf.length; ++i)
        {
            chars[2 * i] = HEX_CHARS[(buf[i] & 0xF0) >>> 4];
            chars[2 * i + 1] = HEX_CHARS[buf[i] & 0x0F];
        }
        return "0x" + new String(chars);
    }


    //Getter Functions
    public String getSha256hex()
    {
        return sha256hex;
    }

    public ByteBuffer getSha256ByteBuffer()
    {
        return UTF8Type.instance.decompose(sha256hex);
    }
}
