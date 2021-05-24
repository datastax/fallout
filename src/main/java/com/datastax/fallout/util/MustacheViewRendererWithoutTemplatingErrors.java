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
package com.datastax.fallout.util;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Locale;

import com.google.common.base.Throwables;
import io.dropwizard.views.View;
import io.dropwizard.views.mustache.MustacheViewRenderer;

public class MustacheViewRendererWithoutTemplatingErrors extends MustacheViewRenderer
{
    /** Ignore thrown exceptions caused by Broken Pipe: it's not wanted in fallout logs, see FAL-1129 */
    @Override
    public void render(View view, Locale locale, OutputStream output) throws IOException
    {
        try
        {
            super.render(view, locale, output);
        }
        catch (Throwable e)
        {
            final Throwable rootCause = Throwables.getRootCause(e);
            if (!(rootCause instanceof IOException && rootCause.getMessage().contains("Broken pipe")))
            {
                throw e;
            }
        }
    }
}
