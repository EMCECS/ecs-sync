/*
 * Copyright (c) 2015-2017 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package sync.ui

import groovy.time.Duration

class DisplayUtil {
    static String simpleSize(Long bytes) {
        if (!bytes) return '0'
        Long base = 1024L
        Integer decimals = 1
        def prefix = ['', 'K', 'M', 'G', 'T']
        int i = Math.log(bytes) / Math.log(base) as Integer
        i = (i >= prefix.size() ? prefix.size() - 1 : i)
        return Math.round((bytes / base**i) * 10**decimals) / 10**decimals + prefix[i]
    }

    static String shortDur(Duration dur) {
        def str = ''
        ['years', 'months', 'days', 'hours', 'minutes', 'seconds'].each {
            if (dur.properties[it]) str += ", ${dur.properties[it]} ${it}"
        }
        str.length() > 2 ? str[2..-1] : str
    }
}
