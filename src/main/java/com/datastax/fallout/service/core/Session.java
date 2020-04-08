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
package com.datastax.fallout.service.core;

import java.util.UUID;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(name = "sessions")
public class Session
{
    @PartitionKey
    private UUID tokenId;

    @Column
    private String userId;

    @Column
    private String tokenType;

    public Session()
    {

    }

    public Session(User user, String tokenType)
    {
        this.tokenId = UUID.randomUUID();
        this.userId = user.getEmail();
        this.tokenType = tokenType;
    }

    public Session(UUID tokenId, String userId, String tokenType)
    {
        this.tokenId = tokenId;
        this.userId = userId;
        this.tokenType = tokenType;
    }

    public UUID getTokenId()
    {
        return tokenId;
    }

    public String getUserId()
    {
        return userId;
    }

    public String getTokenType()
    {
        return tokenType;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Session session = (Session) o;

        if (tokenId != null ? !tokenId.equals(session.tokenId) : session.tokenId != null)
            return false;
        if (tokenType != null ? !tokenType.equals(session.tokenType) : session.tokenId != null)
            return false;
        return !(userId != null ? !userId.equals(session.userId) : session.userId != null);

    }

    @Override
    public int hashCode()
    {
        int result = tokenId != null ? tokenId.hashCode() : 0;
        result = 31 * result + (tokenType != null ? tokenType.hashCode() : 0);
        result = 31 * result + (userId != null ? userId.hashCode() : 0);
        return result;
    }

    public void setUserId(String userId)
    {
        this.userId = userId;
    }

    public void setTokenId(UUID tokenId)
    {
        this.tokenId = tokenId;
    }

    public void setTokenType(String tokenType)
    {
        this.tokenType = tokenType;
    }
}
