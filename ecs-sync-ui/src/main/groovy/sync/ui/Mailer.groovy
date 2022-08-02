/*
 * Copyright (c) 2016-2018 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import groovy.util.logging.Slf4j

@Slf4j
trait Mailer {
    def mailService

    def simpleMail(add, sub, body) {
        try {
            mailService.sendMail {
                to add
                subject sub
                text body
            }
        } catch (Throwable t) {
            log.error("could not send mail [subject: ${sub}]", t)
        }
    }
}
