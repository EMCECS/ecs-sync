/*
 * Copyright (c) 2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DbParams {
    public static DbParams create() {
        return new DbParams();
    }

    private final List<DbParam> dataParams = new ArrayList<>();
    private final List<DbParam> whereClauseParams = new ArrayList<>();

    public DbParams addDataParam(DbField field, Object value) {
        return addDataParam(field, value, false);
    }

    public DbParams addDataParam(DbField field, Object value, boolean rawReference) {
        dataParams.add(new DbParam(field, value, rawReference));
        return this;
    }

    public DbParams removeDataParam(DbField field) {
        dataParams.removeIf(dbParam -> Objects.equals(field, dbParam.field));
        return this;
    }

    public DbParams addWhereClauseParam(DbField field, Object value) {
        whereClauseParams.add(new DbParam(field, value, false));
        return this;
    }

    public DbParams removeWhereClauseParam(String field) {
        whereClauseParams.removeIf(dbParam -> Objects.equals(field, dbParam.field));
        return this;
    }

    public List<DbParam> dataParams() {
        return dataParams;
    }

    public List<DbParam> whereClauseParams() {
        return whereClauseParams;
    }

    /**
     * Extracts all data param fields into an array.
     */
    public List<DbField> toDataFieldList() {
        return dataParams.stream().map(DbParam::field).collect(Collectors.toList());
    }

    /**
     * Extracts all param values into an array.  The array will have the data params first, followed by the where-clause
     * params.
     */
    public Object[] toParamValueArray() {
        return Stream.concat(
                dataParams.stream().filter(dbParam -> !dbParam.rawReference()).map(DbParam::value),
                whereClauseParams.stream().map(DbParam::value)
        ).toArray();
    }

    public static class DbParam {
        private final DbField field;
        private final Object value;
        private final boolean rawReference;

        private DbParam(DbField field, Object value, boolean rawReference) {
            this.field = field;
            this.value = value;
            this.rawReference = rawReference;
        }

        public DbField field() {
            return field;
        }

        public Object value() {
            return value;
        }

        public boolean rawReference() {
            return rawReference;
        }
    }
}
