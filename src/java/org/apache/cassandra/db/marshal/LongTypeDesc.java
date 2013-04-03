/*
 *@author ZhongYu 
 *
 */
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;

import org.apache.cassandra.cql.jdbc.JdbcLong;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.utils.ByteBufferUtil;

public class LongTypeDesc extends AbstractType<Long>
{
    public static final LongTypeDesc instance = new LongTypeDesc();

    LongTypeDesc() {} // singleton

    public Long compose(ByteBuffer bytes)
    {
        return JdbcLong.instance.compose(bytes);
    }

    public ByteBuffer decompose(Long value)
    {
        return JdbcLong.instance.decompose(value);
    }

    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        if (o1.remaining() == 0)
        {
            return o2.remaining() == 0 ? 0 : 1;
        }
        if (o2.remaining() == 0)
        {
            return -1;
        }

        int diff = o2.get(o2.position()) - o1.get(o1.position());
        if (diff != 0)
            return diff;


        return -1 * ByteBufferUtil.compareUnsigned(o1, o2);
    }

    public String getString(ByteBuffer bytes)
    {
        try
        {
            return JdbcLong.instance.getString(bytes);
        }
        catch (org.apache.cassandra.cql.jdbc.MarshalException e)
        {
            throw new MarshalException(e.getMessage());
        }
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        long longTypeDesc;

        try
        {
            longTypeDesc = Long.parseLong(source);
        }
        catch (Exception e)
        {
            throw new MarshalException(String.format("unable to make long from '%s'", source), e);
        }

        return decompose(longTypeDesc);
    }

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        if (bytes.remaining() != 8 && bytes.remaining() != 0)
            throw new MarshalException(String.format("Expected 8 or 0 byte long (%d)", bytes.remaining()));
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.BIGINT2;
    }
}
