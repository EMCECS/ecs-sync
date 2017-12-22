/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package sync.ui

import com.emc.ecs.sync.config.SyncConfig
import grails.validation.Validateable

import javax.xml.bind.annotation.*

@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
class ScheduledSync implements Validateable {
    @XmlElement
    List<Day> daysOfWeek = []
    @XmlElement
    int startHour
    @XmlElement
    int startMinute
    @XmlElement
    config = new SyncConfig()
    @XmlElement
    AlertConfig alerts = new AlertConfig()

    @XmlAccessorType(XmlAccessType.NONE)
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
