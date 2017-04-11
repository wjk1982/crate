/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.doc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.udf.UDFLanguage;
import io.crate.operation.udf.UserDefinedFunctionMetaData;
import io.crate.operation.udf.UserDefinedFunctionService;
import io.crate.operation.udf.UserDefinedFunctionsMetaData;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.junit.Before;
import org.junit.Test;

import javax.script.ScriptException;
import java.util.Map;

import static org.hamcrest.core.Is.is;

public class DocSchemaInfoTest extends CrateUnitTest {

    private UserDefinedFunctionService udfService;
    private DocSchemaInfo docSchemaInfo;

    @Before
    public void setup() throws Exception {
        udfService = new UserDefinedFunctionService(new NoopClusterService());
        udfService.registerLanguage(new UDFLanguage() {
            @Override
            public FunctionImplementation createFunctionImplementation(UserDefinedFunctionMetaData metaData) throws ScriptException {
                // throw ScriptException
                if (!metaData.definition().equals("\"Hello, World!\"Q")) {
                    throw new ScriptException("this is not Burlesque");
                }
                return () -> new FunctionInfo(new FunctionIdent(metaData.schema(), metaData.name(), metaData.argumentTypes()), metaData.returnType());
            }

            @Override
            public void validate(UserDefinedFunctionMetaData metadata) throws Exception {
            }

            @Override
            public String name() {
                return "burlesque";
            }
        });
        docSchemaInfo = new DocSchemaInfo("doc", new NoopClusterService(), null, udfService,
            new TestingDocTableInfoFactory(ImmutableMap.of()));
    }

    @Test
    public void testInvalidFunction() throws Exception {
        UserDefinedFunctionMetaData invalid = new UserDefinedFunctionMetaData(
            "my_schema", "invalid", ImmutableList.of(), DataTypes.INTEGER,
            "burlesque", "this is not valid burlesque code"
        );
        UserDefinedFunctionMetaData valid = new UserDefinedFunctionMetaData(
            "my_schema", "valid", ImmutableList.of(), DataTypes.INTEGER,
            "burlesque", "\"Hello, World!\"Q"
        );
        UserDefinedFunctionsMetaData metaData = UserDefinedFunctionsMetaData.of(invalid, valid);
        // if a functionImpl can't be created, it won't be registered
        Map<FunctionIdent, FunctionImplementation> functionImpl =
            docSchemaInfo.toFunctionImpl(metaData.functionsMetaData(), logger);
        assertThat(functionImpl.size(), is(1));
        // the valid functions will be registered
        assertThat(functionImpl.entrySet().iterator().next().getKey().name(), is("valid"));
    }

}
