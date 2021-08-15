/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.hansql.engine.storage;

import java.io.IOException;
import java.util.Collections;

import org.lealone.hansql.exec.store.AbstractSchemaFactory;
import org.lealone.hansql.exec.store.SchemaConfig;
import org.lealone.hansql.optimizer.schema.SchemaPlus;

public class LealoneSchemaFactory extends AbstractSchemaFactory {

    protected LealoneSchemaFactory(String name) {
        super(name);
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        LealoneSchema schema = new LealoneSchema(Collections.emptyList(), getName());
        SchemaPlus hPlus = parent.add(getName(), schema);
        schema.setHolder(hPlus);
    }
}
