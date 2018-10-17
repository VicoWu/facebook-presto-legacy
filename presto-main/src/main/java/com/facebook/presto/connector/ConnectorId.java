/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class ConnectorId
{
    private static final String INFORMATION_SCHEMA_CONNECTOR_PREFIX = "$info_schema@";
    private static final String SYSTEM_TABLES_CONNECTOR_PREFIX = "$system@";

    private final String catalogName;

    //我们可以看到ConnectorId是用catalogName构造的，比如hive.properties中的connectorId=hive.hadoop2,因此我们可能会遇到异常，
    @JsonCreator
    public ConnectorId(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        checkArgument(!catalogName.isEmpty(), "catalogName is empty");
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConnectorId that = (ConnectorId) o;
        return Objects.equals(catalogName, that.catalogName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return catalogName;
    } //这里toString打印的不是connector的名字，比如hive-hadoop2,而是 catalogName,即catalog目录下面properties文件的前缀

    public static boolean isInternalSystemConnector(ConnectorId connectorId)
    {
        return connectorId.getCatalogName().startsWith(SYSTEM_TABLES_CONNECTOR_PREFIX) ||
                connectorId.getCatalogName().startsWith(INFORMATION_SCHEMA_CONNECTOR_PREFIX);
    }

    public static ConnectorId createInformationSchemaConnectorId(ConnectorId connectorId)
    {
        return new ConnectorId(INFORMATION_SCHEMA_CONNECTOR_PREFIX + connectorId.getCatalogName());
    }

    public static ConnectorId createSystemTablesConnectorId(ConnectorId connectorId)
    {
        return new ConnectorId(SYSTEM_TABLES_CONNECTOR_PREFIX + connectorId.getCatalogName());
    }
}
