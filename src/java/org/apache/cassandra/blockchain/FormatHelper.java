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
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UUIDType;

public class FormatHelper
{
    /***
     * Converts a ByteBuffer to String. If the Sting contains non prinatable symbols, the result will be a hex String instead
     * @param sb ByteBuffer to convert
     * @return Sting or hex String
     */
    public static String convertByteBufferToString(ByteBuffer sb)
    {
        if (sb == null || !sb.hasArray())
            return "<empty>"; //TODO Return ""
        //TODO Optimise this function!
        //System.out.println("Check sb: " + sb);
        byte[] content = Arrays.copyOfRange(sb.array(), sb.position(), sb.limit());
        //byte[] content = new byte[sb.remaining()];
        String valueString = new String(content);
        //String valueString = new String(sb);.

        //If there are non-printable characters, print the value in hex format
        if (!StringUtils.isAsciiPrintable(valueString))
        {
            valueString = asHex(content);
        }
        return valueString;
    }

    ///Helper variable for asHex
    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    /***
     * Transform a buffer into a hex string
     * @param buf Hex Buffer
     * @return String as HexValue
     */
    private static String asHex(byte[] buf)
    {
        char[] chars = new char[2 * buf.length];
        for (int i = 0; i < buf.length; ++i)
        {
            chars[2 * i] = HEX_CHARS[(buf[i] & 0xF0) >>> 4];
            chars[2 * i + 1] = HEX_CHARS[buf[i] & 0x0F];
        }
        return "0x" + new String(chars);
    }

    /***
     * Concatenate two ByteBuffer Arrays
     * @param a first ByteBuffer Array
     * @param b second ByteBuffer Array
     * @return Concatenated ByteBuffer Array
     */
    public static ByteBuffer[] concat(ByteBuffer[] a, ByteBuffer[] b)
    {
        int aLen = a.length;
        int bLen = b.length;
        ByteBuffer[] c = new ByteBuffer[aLen + bLen];
        System.arraycopy(a, 0, c, 0, aLen);
        System.arraycopy(b, 0, c, aLen, bLen);
        return c;
    }

    public static String convertKeyToString(ByteBuffer key)
    {
        String result;
        try
        {
            result = UUIDType.instance.compose(key).toString();
        }
        catch (Exception e)
        {
            //Backup if key is not a UUIDType
            result = convertByteBufferToString(key);
        }
        return result;
    }

    public static ByteBuffer[] removeNull(ByteBuffer[] a)
    {
        int newSize = 0;
        for (ByteBuffer item : a)
        {
            if (item != null)
            {
                newSize++;
            }
        }
        ByteBuffer[] result = new ByteBuffer[newSize];
        int index = 0;
        for (ByteBuffer item : a)
        {
            if (item != null)
            {
                result[index++] = item;
            }
        }
        return result;
    }

    public static ByteBuffer[] addElement(ByteBuffer[] a, ByteBuffer b)
    {
        ByteBuffer[] result = new ByteBuffer[a.length + 1];
        int index = 0;
        for (ByteBuffer item : a)
        {
            result[index++] = item;
        }
        result[index] = b;
        return result;
    }

    public static ByteBuffer[] ListToArray(List<ByteBuffer> a)
    {
        ByteBuffer[] result = new ByteBuffer[a.size()];
        int index = 0;
        for (ByteBuffer item : a)
        {
            result[index++] = item;
        }
        return result;
    }

    public static UntypedResultSet executeQuery(String query)
    {
        UntypedResultSet rs;
        if (HashBlock.getDebug())
        {
            rs = QueryProcessor.executeInternal(query);
        }
        else
        {
            rs = QueryProcessor.execute(query, ConsistencyLevel.ONE);
        }
        return rs;
    }
}
