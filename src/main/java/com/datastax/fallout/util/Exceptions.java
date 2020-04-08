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

import java.io.IOException;
import java.io.UncheckedIOException;

public class Exceptions
{
    public interface InterruptibleSupplier<T>
    {
        T get() throws InterruptedException;
    }

    /** Return the result of interruptible.get(); if it throws InterruptedException, try again until it doesn't */
    public static <T> T getUninterruptibly(InterruptibleSupplier<T> interruptible)
    {
        while (true)
        {
            try
            {
                return interruptible.get();
            }
            catch (InterruptedException ignored)
            {
            }
        }
    }

    public interface InterruptibleRunnable
    {
        void run() throws InterruptedException;
    }

    public static void runUninterruptibly(InterruptibleRunnable interruptible)
    {
        getUninterruptibly(() -> {
            interruptible.run();
            return null;
        });
    }

    public interface CheckedExceptionThrowingSupplier<T>
    {
        T get() throws Throwable;
    }

    /** Return the result of checked.get(), wrapping any exception thrown with a RuntimeException */
    public static <T> T getUnchecked(CheckedExceptionThrowingSupplier<T> checked)
    {
        try
        {
            return checked.get();
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    public interface IOExceptionThrowingSupplier<T>
    {
        T get() throws IOException;
    }

    /** Return the result of checked.get(), wrapping any IOException thrown with an UncheckedIOException */
    public static <T> T getUncheckedIO(IOExceptionThrowingSupplier<T> checked)
    {
        try
        {
            return checked.get();
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    public interface CheckedExceptionThrowingRunnable
    {
        void run() throws Throwable;
    }

    /** Run checked.run(), wrapping any exception thrown with a RuntimeException */
    public static void runUnchecked(CheckedExceptionThrowingRunnable checked)
    {
        try
        {
            checked.run();
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    public interface IOExceptionThrowingRunnable
    {
        void run() throws IOException;
    }

    /** Run checked.run(), wrapping any IOException thrown with an UncheckedIOException */
    public static void runUncheckedIO(IOExceptionThrowingRunnable checked)
    {
        try
        {
            checked.run();
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }
}
