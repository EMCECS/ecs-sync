/*
 * Copyright (c) 2014-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.storage.cas;

import com.filepool.fplibrary.FPLibraryException;
import com.filepool.fplibrary.FPPool;

public class CasPool extends FPPool {
    public CasPool(String[] strings) throws FPLibraryException {
        super(strings);
    }

    public CasPool(String s) throws FPLibraryException {
        super(s);
    }

    public long getPoolRef() {
        return this.mPoolRef;
    }
}