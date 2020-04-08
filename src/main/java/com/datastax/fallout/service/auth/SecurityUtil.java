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
package com.datastax.fallout.service.auth;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;

import com.google.common.annotations.VisibleForTesting;

import com.datastax.driver.core.utils.Bytes;
import com.datastax.fallout.util.ScopedLogger;

//from http://www.javacodegeeks.com/2012/05/secure-password-storage-donts-dos-and.html
public class SecurityUtil
{
    // VERY important to use SecureRandom instead of just Random
    private final SecureRandom random;

    private static ScopedLogger logger = ScopedLogger.getLogger(SecurityUtil.class);

    public static final String DEFAULT_ALGORITHM = "NativePRNGNonBlocking";

    @VisibleForTesting
    public SecurityUtil() throws NoSuchAlgorithmException
    {
        this(DEFAULT_ALGORITHM);
    }

    public SecurityUtil(String algorithm) throws NoSuchAlgorithmException
    {
        random = SecureRandom.getInstance(algorithm);
    }

    public boolean authenticate(String attemptedPassword, ByteBuffer encryptedPassword, ByteBuffer salt)
    {
        // Encrypt the clear-text password using the same salt that was used to
        // encrypt the original password
        ByteBuffer encryptedAttemptedPassword = getEncryptedPassword(attemptedPassword, salt);

        // Authentication succeeds if encrypted password that the user entered
        // is equal to the stored hash
        return encryptedPassword.equals(encryptedAttemptedPassword);
    }

    public ByteBuffer getEncryptedPassword(String password, ByteBuffer salt)
    {
        try (ScopedLogger.Scoped ignored = logger.scopedDebug("getEncryptedPassword"))
        {
            // PBKDF2 with SHA-1 as the hashing algorithm. Note that the NIST
            // specifically names SHA-1 as an acceptable hashing algorithm for PBKDF2
            String algorithm = "PBKDF2WithHmacSHA1";
            // SHA-1 generates 160 bit hashes, so that's what makes sense here
            int derivedKeyLength = 160;
            // Pick an iteration count that works for you. The NIST recommends at
            // least 1,000 iterations:
            // http://csrc.nist.gov/publications/nistpubs/800-132/nist-sp800-132.pdf
            // iOS 4.x reportedly uses 10,000:
            // http://blog.crackpassword.com/2010/09/smartphone-forensics-cracking-blackberry-backup-passwords/
            int iterations = 20000;

            KeySpec spec;
            try (ScopedLogger.Scoped ignored1 = logger.scopedDebug("PBEKeySpec()"))
            {
                spec = new PBEKeySpec(password.toCharArray(), Bytes.getArray(salt), iterations,
                    derivedKeyLength);
            }

            SecretKeyFactory f;

            try (ScopedLogger.Scoped ignored1 = logger.scopedDebug("SecretKeyFactory.getInstance()"))
            {
                f = SecretKeyFactory.getInstance(algorithm);
            }

            try (ScopedLogger.Scoped ignored1 = logger.scopedDebug("generateSecret(spec).getEncoded()"))
            {
                return ByteBuffer.wrap(f.generateSecret(spec).getEncoded());
            }
        }
        catch (NoSuchAlgorithmException | InvalidKeySpecException e)
        {
            throw new RuntimeException("Unexpected password encryption error", e);
        }
    }

    public ByteBuffer generateSalt()
    {
        try (ScopedLogger.Scoped ignored = logger.scopedDebug("generateSalt"))
        {
            // Generate a 8 byte (64 bit) salt as recommended by RSA PKCS5
            byte[] salt = new byte[8];

            logger.doWithScopedDebug(() -> random.nextBytes(salt), "nextBytes");

            return ByteBuffer.wrap(salt);
        }
    }
}
