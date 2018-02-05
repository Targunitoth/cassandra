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
package org.apache.cassandra.transport.messages;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import io.netty.buffer.ByteBuf;
import javafx.util.converter.DateTimeStringConverter;
import javafx.util.converter.TimeStringConverter;
import org.apache.cassandra.blockchain.HashBlock;
import org.apache.cassandra.blockchain.VerifyHashIterative;
import org.apache.cassandra.blockchain.VerifyHashRecursive;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.UUIDGen;

/**
 * A CQL query
 */
public class QueryMessage extends Message.Request
{
    public static final Message.Codec<QueryMessage> codec = new Message.Codec<QueryMessage>()
    {
        public QueryMessage decode(ByteBuf body, ProtocolVersion version)
        {
            String query = CBUtil.readLongString(body);
            return new QueryMessage(query, QueryOptions.codec.decode(body, version));
        }

        public void encode(QueryMessage msg, ByteBuf dest, ProtocolVersion version)
        {
            CBUtil.writeLongString(msg.query, dest);
            if (version == ProtocolVersion.V1)
                CBUtil.writeConsistencyLevel(msg.options.getConsistency(), dest);
            else
                QueryOptions.codec.encode(msg.options, dest, version);
        }

        public int encodedSize(QueryMessage msg, ProtocolVersion version)
        {
            int size = CBUtil.sizeOfLongString(msg.query);

            if (version == ProtocolVersion.V1)
            {
                size += CBUtil.sizeOfConsistencyLevel(msg.options.getConsistency());
            }
            else
            {
                size += QueryOptions.codec.encodedSize(msg.options, version);
            }
            return size;
        }
    };

    public String query;
    public final QueryOptions options;

    public QueryMessage(String query, QueryOptions options)
    {
        super(Type.QUERY);
        this.query = query;
        this.options = options;
    }

    public Message.Response execute(QueryState state, long queryStartNanoTime)
    {
        try
        {
            //Hack here for validate Blockchain
            if (QueryProcessor.validateTable(query)) return new ResultMessage.Void();

            if (options.getPageSize() == 0)
                throw new ProtocolException("The page size cannot be 0");

            //Hack here for cqlsh insert command
            if (query.toUpperCase().contains("INSERT") && query.contains(HashBlock.getBlockchainIDString()))
            {
                //TODO Hack Here for sign()!!!!! as value
                if(query.toUpperCase().contains("SIGN(")){
                    query = QueryProcessor.generateSignature(query);
                }

                manipulateQuery(queryStartNanoTime);
                System.out.println("My new Query: " + query);
            }

            UUID tracingId = null;
            if (isTracingRequested())
            {
                tracingId = UUIDGen.getTimeUUID();
                state.prepareTracingSession(tracingId);
            }

            if (state.traceNextQuery())
            {
                state.createTracingSession(getCustomPayload());

                ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
                builder.put("query", query);
                if (options.getPageSize() > 0)
                    builder.put("page_size", Integer.toString(options.getPageSize()));
                if (options.getConsistency() != null)
                    builder.put("consistency_level", options.getConsistency().name());
                if (options.getSerialConsistency() != null)
                    builder.put("serial_consistency_level", options.getSerialConsistency().name());

                Tracing.instance.begin("Execute CQL3 query", state.getClientAddress(), builder.build());
            }

            Message.Response response = ClientState.getCQLQueryHandler().process(query, state, options, getCustomPayload(), queryStartNanoTime);
            if (options.skipMetadata() && response instanceof ResultMessage.Rows)
                ((ResultMessage.Rows) response).result.metadata.setSkipMetadata();

            if (tracingId != null)
                response.setTracingId(tracingId);

            return response;
        }
        catch (Exception e)
        {
            e.printStackTrace();
            JVMStabilityInspector.inspectThrowable(e);
            if (!((e instanceof RequestValidationException) || (e instanceof RequestExecutionException)))
                logger.error("Unexpected error during query", e);
            return ErrorMessage.fromException(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    private void manipulateQuery(long queryStartNanoTime)
    {
        String TableString = "";
        //Skip Nr. 1 (key) and Nr. 2 (signature)
        for (int i = 2; i < HashBlock.tables.length; i++)
        {
            TableString += ", " + HashBlock.tables[i];
        }

        String nullString = "";
        for (int i = 2; i < HashBlock.tables.length; i++)
        {
            nullString += ", null";
        }

        //TODO Optional generate Hashblock here.
        //TODO change HashBlock Timestamp to long
        //TimeType.instance.decompose(queryStartNanoTime )
        String[] querryArray = query.split("\\)");
        if (querryArray.length == 3)
        {
            query = querryArray[0] + TableString + ")" + querryArray[1] + nullString +")" + querryArray[2];
        }
        else if (querryArray.length == 2)
        { //Without Semicolon
            query = querryArray[0] + TableString + ")" + querryArray[1] + nullString +")";
        }
        else if (querryArray.length == 4)
        { //Case with now
            //querryArray[1] => now(
            query = querryArray[0] + TableString + ")" + querryArray[1] + ")" + querryArray[2] + nullString +")" + querryArray[3];
        }
        else
        {
            throw new IndexOutOfBoundsException();
        }
    }

    @Override
    public String toString()
    {
        return "QUERY " + query + "[pageSize = " + options.getPageSize() + "]";
    }
}
