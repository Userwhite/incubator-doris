// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.backup.CatalogMocker;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.CatalogRecycleBin;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;

import com.google.common.collect.ImmutableList;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShowDataCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private static final TableNameInfo tableNameInfo =
            new TableNameInfo(internalCtl, CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
    private static final OlapTable olapTable = new OlapTable(CatalogMocker.TEST_TBL_ID,
            CatalogMocker.TEST_TBL_NAME,
            CatalogMocker.TEST_TBL_BASE_SCHEMA,
            KeysType.AGG_KEYS,
            new SinglePartitionInfo(),
            new RandomDistributionInfo(32));
    @Injectable
    private Env env;
    @Mocked
    private InternalCatalog catalog;
    @Mocked
    private AccessControllerManager accessControllerManager;
    @Mocked
    private ConnectContext connectContext;
    @Mocked
    private Database database;

    @Test
    public void testValidateShowDataNormal() throws Exception {
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return env;
            }
        };
        Database db = CatalogMocker.mockDb();
        new Expectations() {
            {
                catalog.getDb(anyString);
                minTimes = 0;
                result = db;

                database.getTableOrMetaException(tableNameInfo.getTbl(), TableIf.TableType.OLAP);
                minTimes = 0;
                result = olapTable;

                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.SHOW);
                minTimes = 0;
                result = true;

                accessControllerManager.checkTblPriv(connectContext, tableNameInfo, PrivPredicate.SHOW);
                minTimes = 0;
                result = true;
            }
        };

        SlotReference tableName = new SlotReference("TableName", IntegerType.INSTANCE);
        List<OrderKey> keys = ImmutableList.of(
                new OrderKey(tableName, true, false)
        );

        TableNameInfo tableNameInfo =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);

        Map<String, String> properties = new HashMap<>();
        ShowDataCommand command = new ShowDataCommand(tableNameInfo, keys, properties, false);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        // Ensure show data result includes binlog columns in metadata.
        Assertions.assertTrue(command.getMetaData().getColumns().stream()
                        .anyMatch(c -> c.getName().equalsIgnoreCase("BinlogSize")),
                "SHOW DATA should contain BinlogSize column");
    }

    @Test
    public void testValidateShowAllDataNormal() throws Exception {
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return env;
            }

            @Mock
            public InternalCatalog getCurrentInternalCatalog() {
                return catalog;
            }
        };
        Database db = CatalogMocker.mockDb();
        new Expectations() {
            {
                catalog.getDbOrAnalysisException(anyString);
                minTimes = 0;
                result = db;

                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.getDatabase();
                minTimes = 0;
                result = CatalogMocker.TEST_DB_NAME;

                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;
            }
        };

        SlotReference tableName = new SlotReference("TableName", IntegerType.INSTANCE);
        List<OrderKey> keys = ImmutableList.of(
                new OrderKey(tableName, true, false)
        );

        Map<String, String> properties = new HashMap<>();
        ShowDataCommand command = new ShowDataCommand(null, keys, properties, false);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        Assertions.assertTrue(command.getMetaData().getColumns().stream()
                        .anyMatch(c -> c.getName().equalsIgnoreCase("BinlogSize")),
                "SHOW DATA should contain BinlogSize column");
    }

    @Test
    public void testValidateShowAllDataGetAllDbStats(@Mocked OlapTable t1, @Mocked OlapTable t2) throws Exception {
        CatalogRecycleBin localRecycleBin = new CatalogRecycleBin();
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return env;
            }

            @Mock
            public InternalCatalog getCurrentInternalCatalog() {
                return catalog;
            }

            @Mock
            public CatalogRecycleBin getCurrentRecycleBin() {
                return localRecycleBin;
            }
        };
        // Use mocked databases so that (Database) cast in getAllDbStats() works.
        new Expectations() {
            {
                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.getDatabase();
                minTimes = 0;
                result = "";

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;

                catalog.getDbNames();
                minTimes = 0;
                result = ImmutableList.of("db1", "db2");
            }
        };

        // Build two mocked databases
        Database db1 = CatalogMocker.mockDb();
        Database db2 = CatalogMocker.mockDb();

        new Expectations(db1, db2, t1, t2) {
            {
                catalog.getDbNullable("db1");
                minTimes = 0;
                result = db1;

                catalog.getDbNullable("db2");
                minTimes = 0;
                result = db2;

                db1.getId();
                minTimes = 0;
                result = 101L;
                db1.getUsedDataSize();
                minTimes = 0;
                result = Pair.of(10L, 1L);
                db1.getTables();
                minTimes = 0;
                result = ImmutableList.of(t1);

                db1.readLock();
                minTimes = 0;
                db1.readUnlock();
                minTimes = 0;

                db2.getId();
                minTimes = 0;
                result = 102L;
                db2.getUsedDataSize();
                minTimes = 0;
                result = Pair.of(20L, 2L);
                db2.getTables();
                minTimes = 0;
                result = ImmutableList.of(t2);

                db2.readLock();
                minTimes = 0;
                db2.readUnlock();
                minTimes = 0;

                t1.isManagedTable();
                minTimes = 0;
                result = true;
                t2.isManagedTable();
                minTimes = 0;
                result = true;

                t1.getBinlogSize();
                minTimes = 0;
                result = 5L;
                t2.getBinlogSize();
                minTimes = 0;
                result = 7L;
            }
        };

        SlotReference tableName = new SlotReference("TableName", IntegerType.INSTANCE);
        List<OrderKey> keys = ImmutableList.of(new OrderKey(tableName, true, false));
        ShowDataCommand command = new ShowDataCommand(null, keys, new HashMap<>(), false);

        ShowResultSet rs = command.doRun(connectContext, null);
        List<List<String>> rows = rs.getResultRows();

        // db1, db2, total
        Assertions.assertEquals(3, rows.size());
        Assertions.assertEquals(ImmutableList.of("101", "db1", "10", "1", "5", "0", "0"), rows.get(0));
        Assertions.assertEquals(ImmutableList.of("102", "db2", "20", "2", "7", "0", "0"), rows.get(1));
        Assertions.assertEquals(ImmutableList.of("Total", "NULL", "30", "3", "12", "0", "0"), rows.get(2));
    }

    @Test
    void testValidateNoPrivilege() throws Exception {
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return env;
            }
        };
        Database db = CatalogMocker.mockDb();
        new Expectations() {
            {
                catalog.getDb(anyString);
                minTimes = 0;
                result = db;

                database.getTableOrMetaException(tableNameInfo.getTbl(), TableIf.TableType.OLAP);
                minTimes = 0;
                result = olapTable;

                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.SHOW);
                minTimes = 0;
                result = true;

                accessControllerManager.checkTblPriv(connectContext, tableNameInfo, PrivPredicate.SHOW);
                minTimes = 0;
                result = false;
            }
        };

        SlotReference tableName = new SlotReference("TableName", IntegerType.INSTANCE);
        List<OrderKey> keys = ImmutableList.of(
                new OrderKey(tableName, true, false)
        );

        // test not exist table
        TableNameInfo tableNameInfoNotExist =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, "tbl_not_exist");

        Map<String, String> properties = new HashMap<>();
        ShowDataCommand command = new ShowDataCommand(tableNameInfoNotExist, keys, properties, false);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext));

        // test no priv
        ShowDataCommand command2 = new ShowDataCommand(tableNameInfo, keys, properties, false);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(connectContext));
    }
}
