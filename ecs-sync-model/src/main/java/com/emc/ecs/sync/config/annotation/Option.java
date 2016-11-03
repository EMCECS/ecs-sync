/*
 * Copyright 2013-2016 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.config.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static com.emc.ecs.sync.config.annotation.Option.Location.CLI;
import static com.emc.ecs.sync.config.annotation.Option.Location.Form;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Option {
    boolean required() default false;

    String label() default "";

    /**
     * Use to restrict the presentation of an option to only CLI or web
     */
    Location[] locations() default {CLI, Form};

    String cliName() default "";

    /**
     * Use for booleans whose default is true. The CLI option will be inverted to disable the property
     */
    boolean cliInverted() default false;

    String description() default "";

    ValueType valueType() default ValueType.NoValue;

    /**
     * Use to constrain the values to a specific list
     */
    String[] valueList() default {};

    String valueHint() default "";

    FormType formType() default FormType.Infer;

    enum ValueType {
        NoValue, SingleValue, MultiValue
    }

    enum FormType {
        Infer, Text, TextArea, Checkbox, Radio, Select
    }

    enum Location {
        CLI, Form
    }
}
