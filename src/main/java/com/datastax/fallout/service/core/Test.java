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

import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.io.FileUtils;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import static com.datastax.fallout.harness.TestDefinition.getDereferencedYaml;

@Table(name = "tests")
public class Test
{
    @PartitionKey()
    private String owner;

    @ClusteringColumn
    private String name;

    @Column
    private UUID testId;

    @Column
    private Date createdAt;

    @Column
    private Date lastRunAt;

    @Column
    private String definition;

    @Column
    private Set<String> tags;

    @Column
    private Long sizeOnDiskBytes;

    /** Protected ctor for serialization usage only; use {@link #createTest} to create instances in code */
    protected Test()
    {
    }

    public static Test createTest(String owner, String name, String definition)
    {
        final Test test = new Test();
        test.setOwner(owner);
        test.setName(name);
        test.setDefinition(definition);

        test.setTestId(UUID.randomUUID());
        test.setCreatedAt(new Date());
        // This is not initialized during default construction because we need to be able to detect existing null
        // values that have been deserialized from the database and correct them.
        test.setSizeOnDiskBytes(0L);

        return test;
    }

    public UUID getTestId()
    {
        return testId;
    }

    public void setTestId(UUID testId)
    {
        this.testId = testId;
    }

    public String getOwner()
    {
        return owner;
    }

    public void setOwner(String owner)
    {
        this.owner = owner;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public Date getCreatedAt()
    {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt)
    {
        this.createdAt = createdAt;
    }

    public Date getLastRunAt()
    {
        return lastRunAt;
    }

    public void setLastRunAt(Date lastRunAt)
    {
        this.lastRunAt = lastRunAt;
    }

    public String getDefinition()
    {
        return definition;
    }

    public void setDefinition(String yaml)
    {
        this.definition = yaml;
    }

    public Set<String> getTags()
    {
        return tags != null ? tags : Collections.EMPTY_SET;
    }

    public void setTags(Set<String> tags)
    {
        this.tags = tags;
    }

    public Long getSizeOnDiskBytes()
    {
        return this.sizeOnDiskBytes;
    }

    public void setSizeOnDiskBytes(Long sizeOnDiskBytes)
    {
        this.sizeOnDiskBytes = sizeOnDiskBytes;
    }

    @JsonIgnore
    public String getSizeOnDisk()
    {
        return sizeOnDiskBytes != null ? FileUtils.byteCountToDisplaySize(sizeOnDiskBytes) : "Unknown";
    }

    public boolean isOwnedBy(User u)
    {
        return u != null && getOwner().equalsIgnoreCase(u.getEmail());
    }

    public TestRun createTestRun(Map<String, Object> templateParams)
    {
        return new TestRun(owner, name, getDereferencedYaml(definition), templateParams);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Test test = (Test) o;

        if (testId != null ? !testId.equals(test.testId) : test.testId != null)
            return false;
        if (owner != null ? !owner.equals(test.owner) : test.owner != null)
            return false;
        if (name != null ? !name.equals(test.name) : test.name != null)
            return false;
        if (createdAt != null ? !createdAt.equals(test.createdAt) : test.createdAt != null)
            return false;
        return !(definition != null ? !definition.equals(test.definition) : test.definition != null);

    }

    @Override
    public int hashCode()
    {
        int result = testId != null ? testId.hashCode() : 0;
        result = 31 * result + (owner != null ? owner.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (createdAt != null ? createdAt.hashCode() : 0);
        result = 31 * result + (definition != null ? definition.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "Test{" +
            "testId=" + testId +
            ", owner='" + owner + '\'' +
            ", name='" + name + '\'' +
            ", createdAt=" + createdAt +
            ", testYaml='" + definition + '\'' +
            ", sizeOnDisk=" + sizeOnDiskBytes +
            '}';
    }
}
