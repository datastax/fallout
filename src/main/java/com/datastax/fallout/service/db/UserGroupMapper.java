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
package com.datastax.fallout.service.db;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.datastax.fallout.service.core.User;

public class UserGroupMapper
{
    public record UserGroup(String name, String prettyName, String userGroupEmail, boolean isNotOther) {

        public static final String OTHER = "OTHER";

        public static UserGroup of(String name, String prettyName, String email)
        {
            return new UserGroup(name, prettyName, email, true);
        }

        /** Special "other" value indicating this is not to take part in CI group lookup,
         *  but should be available as a value for selection */
        public static UserGroup other(String prettyName, String email)
        {
            return new UserGroup(OTHER, prettyName, email, false);
        }
    }

    private final Map<String, UserGroup> groups;

    public UserGroupMapper(List<UserGroup> groups)
    {
        this.groups = Stream
            .concat(
                groups.stream(),
                Stream.of(UserGroup.other("Other", "")))
            .collect(Collectors.toMap(userGroup -> userGroup.name(), Function.identity(),
                // Prefer existing vals to new: this lets an explicit OTHER override the default
                (existingVal, newVal) -> existingVal,
                // Preserve the order of entries
                LinkedHashMap::new));
    }

    public static UserGroupMapper empty()
    {
        return new UserGroupMapper(List.of());
    }

    public Optional<UserGroup> findGroup(@Nullable String groupName)
    {
        return Optional.ofNullable(groupName)
            .flatMap(groupName_ -> Optional.ofNullable(groups.get(groupName_)))
            .filter(UserGroup::isNotOther);
    }

    public String validGroupOrOther(String groupName)
    {
        return findGroup(groupName).map(userGroup -> userGroup.name()).orElse(UserGroup.OTHER);
    }

    public Collection<UserGroup> getGroups()
    {
        return groups.values();
    }

    public boolean isCIUser(User user)
    {
        // A CI user is in it's own group
        return isInGroupOfCIUser(user, user.getEmail());
    }

    public boolean isInGroupOfCIUser(User user, String potentialCIUserEmail)
    {
        return findGroup(user.getGroup())
            .map(userGroup -> userGroup.userGroupEmail().equalsIgnoreCase(potentialCIUserEmail))
            .orElse(false);
    }
}
