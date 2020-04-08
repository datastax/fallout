/*
 * Copyright 2020 DataStax, Inc.
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
package com.datastax.fallout.util;

import java.util.ArrayList;
import java.util.List;

import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;

public class ShellUtils
{
    // Lifted from http://stackoverflow.com/a/26538384/322152
    private static final Escaper SHELL_QUOTE_ESCAPER;

    private static final Escaper POWERSHELL_QUOTE_ESCAPER;

    private static final Escaper JSON_ESCAPER;

    static
    {
        Escapers.Builder builder = Escapers.builder();
        builder.addEscape('\'', "'\"'\"'");
        SHELL_QUOTE_ESCAPER = builder.build();

        builder = Escapers.builder();
        builder.addEscape('\'', "''");
        POWERSHELL_QUOTE_ESCAPER = builder.build();

        builder = Escapers.builder();
        builder.addEscape('"', "\\\"");
        builder.addEscape('\\', "\\\\");
        JSON_ESCAPER = builder.build();
    }

    // This is from http://stackoverflow.com/a/20725050/322152.  We're not using org.apache.commons.exec.CommandLine
    // because it fails to parse "run 'echo "foo"'" correctly (v1.3 misses off the final ')
    public static String[] split(CharSequence string)
    {
        List<String> tokens = new ArrayList<>();
        boolean escaping = false;
        char quoteChar = ' ';
        boolean quoting = false;
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < string.length(); i++)
        {
            char c = string.charAt(i);
            if (escaping)
            {
                current.append(c);
                escaping = false;
            }
            else if (c == '\\' && !(quoting && quoteChar == '\''))
            {
                escaping = true;
            }
            else if (quoting && c == quoteChar)
            {
                quoting = false;
            }
            else if (!quoting && (c == '\'' || c == '"'))
            {
                quoting = true;
                quoteChar = c;
            }
            else if (!quoting && Character.isWhitespace(c))
            {
                if (current.length() > 0)
                {
                    tokens.add(current.toString());
                    current = new StringBuilder();
                }
            }
            else
            {
                current.append(c);
            }
        }
        if (current.length() > 0)
        {
            tokens.add(current.toString());
        }
        return tokens.toArray(new String[] {});
    }

    public static String escape(String param)
    {
        return escape(param, false);
    }

    public static String escape(String param, boolean forceQuote)
    {
        String escapedQuotesParam = SHELL_QUOTE_ESCAPER.escape(param);
        return forceQuote || escapedQuotesParam.contains(" ") ?
            "'" + escapedQuotesParam + "'" :
            escapedQuotesParam;
    }

    /**
     * Ensure that @param, when embedded in an existing JSON string, will not make that string invalid.
     * It does this by escaping any characters that could terminate the string early.
     */
    public static String escapeJSON(String param)
    {
        return JSON_ESCAPER.escape(param);
    }

    public static String escapePowershell(String param)
    {
        return "'" + POWERSHELL_QUOTE_ESCAPER.escape(param) + "'";
    }

    /** Generate a string that, when used in the shell, will have any escapes
     *  evaluated once, but will preserve the output as a single token: this has the effect of
     *  expanding environment variables. */
    public static String expandVars(String param)
    {
        return String.format("\"$(eval echo %s)\"", param);
    }
}
