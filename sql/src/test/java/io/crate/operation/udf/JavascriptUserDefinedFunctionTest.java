/*
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate.io licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * To enable or use any of the enterprise features, Crate.io must have given
 * you permission to enable and use the Enterprise Edition of CrateDB and you
 * must have a valid Enterprise or Subscription Agreement with Crate.io.  If
 * you enable or use features that are part of the Enterprise Edition, you
 * represent and warrant that you have a valid Enterprise or Subscription
 * Agreement with Crate.io.  Your use of features of the Enterprise Edition
 * is governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.operation.udf;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.analyze.symbol.Literal;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionResolver;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.Matchers.containsString;

public class JavascriptUserDefinedFunctionTest extends AbstractScalarFunctionsTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private static final String JS = "javascript";

    private Map<String, FunctionResolver> functionResolvers = new HashMap<>();
    private Map<FunctionIdent, FunctionImplementation> functionImplementations = new HashMap<>();

    private void registerUserDefinedFunction(String schema, String name, DataType returnType, List<DataType> types, String definition) {
        String fName = String.format("%s.%s", schema, name);
        UserDefinedFunctionMetaData udfMeta = new UserDefinedFunctionMetaData(
            schema,
            name,
            types.stream().map(FunctionArgumentDefinition::of).collect(Collectors.toList()),
            returnType,
            JS,
            definition
        );
        functionImplementations.put(new FunctionIdent(fName, types), UserDefinedFunctionFactory.of(udfMeta));
        functionResolvers.putAll(functions.generateFunctionResolvers(functionImplementations));
        functions.registerSchemaFunctionResolvers(schema, functionResolvers);
    }

    @After
    public void after() {
        functionImplementations.clear();
        functionResolvers.clear();
    }

    @Test
    public void testObjectReturnType() {
        registerUserDefinedFunction("custom", "f", DataTypes.OBJECT, ImmutableList.of(),
            "function f() { return JSON.parse('{\"foo\": \"bar\"}'); }");
        assertEvaluate("custom.f()", new HashMap<String, String>() {{
            put("foo", "bar");
        }});
    }

    @Test
    public void testArrayReturnType() {
        registerUserDefinedFunction("doc", "f", DataTypes.DOUBLE_ARRAY, ImmutableList.of(), "function f() { return [1, 2]; }");
        assertEvaluate("f()", new double[]{1.0, 2.0});
    }

    @Test
    public void testTimestampReturnType() {
        registerUserDefinedFunction("doc", "f", DataTypes.TIMESTAMP, ImmutableList.of(),
            "function f() { return \"1990-01-01T00:00:00\"; }");
        assertEvaluate("f()", 631152000000L);
    }

    @Test
    public void testIpReturnType() {
        registerUserDefinedFunction("doc", "f", DataTypes.IP, ImmutableList.of(), "function f() { return \"127.0.0.1\"; }");
        assertEvaluate("f()", DataTypes.IP.value("127.0.0.1"));
    }

    @Test
    public void testPrimitiveReturnType() {
        registerUserDefinedFunction("doc", "f", DataTypes.INTEGER, ImmutableList.of(), "function f() { return 10; }");
        assertEvaluate("f()", 10);
    }

    @Test
    public void testObjectReturnTypeAndInputArguments() {
        registerUserDefinedFunction("doc", "f", DataTypes.FLOAT, ImmutableList.of(DataTypes.DOUBLE, DataTypes.SHORT),
            "function f(x, y) { return x + y; }");
        assertEvaluate(" f(double_val, short_val)", 3.0f, Literal.of(1), Literal.of(2));
    }

    @Test
    public void testPrimitiveReturnTypeAndInputArguments() {
        registerUserDefinedFunction("doc", "f", DataTypes.FLOAT, ImmutableList.of(DataTypes.DOUBLE, DataTypes.SHORT),
            "function f(x, y) { return x + y; }");
        assertEvaluate(" f(double_val, short_val)", 3.0f, Literal.of(1), Literal.of(2));
    }

    @Test
    public void testGeoTypeReturnTypeWithDoubleArray() {
        registerUserDefinedFunction("doc", "f", DataTypes.GEO_POINT, ImmutableList.of(), "function f() { return [1, 1]; }");
        assertEvaluate("f()", new double[]{1.0, 1.0});
    }

    @Test
    public void testGeoTypeReturnTypeWithWKT() {
        registerUserDefinedFunction("doc", "f", DataTypes.GEO_POINT, ImmutableList.of(),
            "function f() { return \"POINT (1.0 2.0)\"; }");
        assertEvaluate("f()", new double[]{1.0, 2.0});
    }

    @Test
    public void testOverloadingUserDefinedFunctions() {
        registerUserDefinedFunction("doc", "f", DataTypes.LONG, ImmutableList.of(), "function f() { return 1; }");
        registerUserDefinedFunction("doc", "f", DataTypes.LONG, ImmutableList.of(DataTypes.LONG), "function f(x) { return x; }");
        registerUserDefinedFunction("doc", "f", DataTypes.LONG, ImmutableList.of(DataTypes.LONG, DataTypes.INTEGER),
            "function f(x, y) { return x + y; }");
        assertEvaluate("f()", 1L);
        assertEvaluate("f(x)", 2L, Literal.of(2));
        assertEvaluate("f(x, a)", 3L, Literal.of(2), Literal.of(1));
    }

    @Test
    public void testFunctionWrongNameInFunctionBody() {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage(containsString("The name [test] of the function signature doesn't"));
        registerUserDefinedFunction("doc", "test", DataTypes.LONG, ImmutableList.of(), "function f() { return 1; }");
        assertEvaluate("test" +
            "()", 1L);
    }

    @Test
    public void testCompilableButIncorrectFunctionBody() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(containsString("The function definition cannot be evaluated"));
        registerUserDefinedFunction("doc", "f", DataTypes.OBJECT, ImmutableList.of(),
            "function f() { return JSON.parse('{\"foo\": a}'); }");
        assertEvaluate("f()", 1L);
    }

    @Test
    public void testNormalizeOnObjectInput() throws Exception {
        registerUserDefinedFunction("custom", "f", DataTypes.LONG, ImmutableList.of(DataTypes.OBJECT),
            "function f(x) { return x; }");
        assertNormalize("custom.f({})", isFunction("custom.f", ImmutableList.of(DataTypes.OBJECT)));
    }

    @Test
    public void testNormalizeOnArrayInput() throws Exception {
        registerUserDefinedFunction("doc", "f", DataTypes.LONG, ImmutableList.of(DataTypes.DOUBLE_ARRAY),
            "function f(x) { return x[1]; }");
        assertNormalize("doc.f([1.0, 2.0])", isLiteral(2L));
    }

    @Test
    public void testNormalizeOnStringInputs() throws Exception {
        registerUserDefinedFunction("doc", "f", DataTypes.LONG, ImmutableList.of(DataTypes.STRING, DataTypes.STRING),
            "function f(x, y) { return x.concat(y); }");
        assertNormalize("f('foo', 'bar')",
            isFunction("doc.f", ImmutableList.of(DataTypes.STRING, DataTypes.STRING)));
    }
}
