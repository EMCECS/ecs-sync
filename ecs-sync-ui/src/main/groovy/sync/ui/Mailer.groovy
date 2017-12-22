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
