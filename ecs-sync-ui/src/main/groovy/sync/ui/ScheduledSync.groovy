/*
 * Copyright (c) 2016-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import com.emc.ecs.sync.config.SyncConfig
import grails.validation.Validateable
import groovy.transform.EqualsAndHashCode

import javax.xml.bind.annotation.*

@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
@EqualsAndHashCode
class ScheduledSync implements Validateable {
    @XmlElement
    List<Day> daysOfWeek = []
    @XmlElement
    int startHour
    @XmlElement
    int startMinute
    @XmlElement
    SyncConfig config = new SyncConfig()
    @XmlElement
    AlertConfig alerts = new AlertConfig()
    @XmlElement
    String preCheckScript

    @XmlAccessorType(XmlAccessType.NONE)
    @EqualsAndHashCode
    static class AlertConfig {
        @XmlElement
        boolean onStart
        @XmlElement
        boolean onComplete
        @XmlElement
        boolean onError
    }

    @XmlEnum
    static enum Day {
        Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday
    }

    static constraints = {
        daysOfWeek size: 1..7
    }
}
