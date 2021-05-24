/*
 * Copyright 2021 DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.fallout.harness;

import java.io.IOException;

import clojure.lang.APersistentMap;
import clojure.lang.Keyword;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.net.MediaType;

import static com.datastax.fallout.harness.JepsenApi.*;

/**
 * An Operation is an atomic member of the history of a test run. At the conclusion of a test run,
 * the history is checked by a Checker.
 *
 * Because operations in Jepsen are unconstrained maps, we attempt to impose some structure in this interop
 * boundary. All operations should have :process, :time, :type keys and possibly a :value key. We provide access
 * to all of these in this class and convert Keywords to Strings.
 *
 * In case a misbehaving Client implemented in Clojure changes this structure, we embed the original Jepsen op in this
 * structure for debugging.
 *
 * Before handing of the Jepsen history to a Checker, we convert it from a seq of maps to a Collection of Operations.
 *
 * @see Checker
 */

@JsonSerialize(using = Operation.JsonSerializer.class)
public class Operation
{
    /**
     * Enum contained standardized types for operations. This makes it easier to write checkers that work for multiple
     * modules by constraining the values of type for an operation. In Jepsen, the :type key can be set to anything but
     * is implicitly standardized to be one of :info, :invoke, :ok, :fail.
     *
     * Because we have less granular modules than most Jepsen tests, we follow some different
     * conventions in Fallout.
     *
     * Type.start is the type of "start" operations used to start modules. You should not emit an O
     * peration with type Type.start unless you know exactly why you're doing so. Type.start operations
     * serve as markers in the history to show when a module started.
     *
     * Type.end is the type of "end" operations when a module's run method completes. You should not
     * emit an Operation with type Type.finish unless you know exactly why you're doing so. Type.end
     * operations serve as markers in the history to show when a module finished.
     *
     * There are Types intended to be emitted by modules.
     *
     * Type.invoke should be used to mark the start of a certain part of a module's run. For example,
     * in a stress module, each run of stress could be marked by a Type.invoke operation.
     *
     * Type.fail should be used for operations that outright failed and shouldn't affect the property
     * under test. For example, a Module performing Cassandra writes should return Type.fail on an
     * UnavailableException.
     *
     * Type.ok should be used for operations that succeeded.
     *
     * Type.info should be used for operations that have left the system in an unknown state or other
     * miscellaneous information. For example, a Module performing Cassandra writes should return
     * Type.info on a WriteTimeoutException.
     *
     * Type.error is appended to the history when the run method throws an unhandled exception or doesn't
     * emit any values.
     * Fallout automatically fails histories that contain an error.
     */
    public enum Type
    {
        start, info, invoke, ok, fail, end, error
    }

    private APersistentMap jepsenOp;

    private MediaType mediaType;
    private Module module;
    private String process;
    private long time;
    private Type type;
    private Object value;

    public Operation(String process, Module module, long time, Type type, MediaType mediaType, Object value)
    {
        this.module = module;
        this.process = process;
        this.time = time;
        this.type = type;
        this.mediaType = mediaType;
        this.value = value;
    }

    static Operation fromOpMap(APersistentMap opMap)
    {
        MediaType mt = (MediaType) opMap.get(MEDIATYPE);

        Operation op = new Operation(
            ((Keyword) opMap.get(PROCESS)).getName(),
            (Module) opMap.get(MODULE),
            (Long) opMap.get(TIME),
            Type.valueOf(((Keyword) opMap.get(TYPE)).getName()),
            mt == null ? MediaType.OCTET_STREAM : mt,
            opMap.get(VALUE));

        op.jepsenOp = opMap;

        return op;
    }

    public MediaType getMediaType()
    {
        return mediaType;
    }

    public Module getModule()
    {
        return module;
    }

    public Object getValue()
    {
        return value;
    }

    public Type getType()
    {
        return type;
    }

    public long getTime()
    {
        return time;
    }

    public String getProcess()
    {
        return process;
    }

    public APersistentMap getJepsenOp()
    {
        return jepsenOp;
    }

    public String toString(boolean formatted)
    {
        String res = "Operation{";
        if (formatted)
        {
            res += "time=" + String.format("%-15s", time);
            res += ", process=" + String.format("%-20s", process);
            res += ", module=" + String.format("%-10s", module);
            res += ", type=" + String.format("%-7s", type);
        }
        else
        {
            res += "time=" + time;
            res += ", process=" + process;
            res += ", module=" + module;
            res += ", type=" + type;
        }
        if (value != null)
        {
            res += ", mediaType=\"" + mediaType + "\"";
            res += ", value=\"" + value.toString().replace("\n", "\\n") + "\"";
        }
        res += '}';
        return res;
    }

    @Override
    public String toString()
    {
        return toString(false);
    }

    public static class JsonSerializer extends StdSerializer<Operation>
    {
        JsonSerializer()
        {
            super(Operation.class);
        }

        @Override
        public void serialize(Operation operation, JsonGenerator gen, SerializerProvider provider) throws IOException
        {
            gen.writeStartObject();
            gen.writeNumberField("time", operation.time);
            gen.writeStringField("process", operation.process);
            gen.writeStringField("module", operation.module.name());
            gen.writeStringField("type", operation.type.toString());
            if (operation.value != null && operation.mediaType != null)
            {
                gen.writeStringField("mediaType", operation.mediaType.toString());
                gen.writeStringField("value", operation.value.toString());
            }
            gen.writeEndObject();
        }
    }
}
