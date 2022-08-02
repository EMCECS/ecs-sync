/*
 * Copyright (c) 2017-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import com.emc.ecs.sync.config.*
import com.emc.ecs.sync.config.annotation.Option.FormType
import groovy.xml.MarkupBuilder

class SyncTagLib {
    static namespace = 'sync'
    static defaultEncodeAs = [taglib: 'none']
    //static encodeAsForTags = [tagName: [taglib:'html'], otherTagName: [taglib:'none']]

    def syncConfig = { attrs ->
        def bean = attrs.bean
        def cssClass = attrs.cssClass
        def prefix = attrs.prefix
        def filterDivId = "_${prefix}.filterDiv_"
        def syncOptionsDivId = "_${prefix}.syncOptionsDiv_"
        def storageWrappers = ConfigUtil.allStorageConfigWrappers()
        def filterWrappers = ConfigUtil.allFilterConfigWrappers()
        def optionWrapper = ConfigUtil.wrapperFor(SyncOptions.class)

        if (!bean) bean = new SyncConfig()

        def mb = new MarkupBuilder(out)

        mb.table(class: "table table-condensed kv-table ${cssClass ?: ''}") {
            tr(class: cssClass ?: '') {
                th { mkp.yieldUnescaped('Job Name:') }
                td {
                    def field = g.textField(name: "${prefix}.jobName", value: bean.jobName, size: 80, placeholder: '(optional)',
                            autocomplete: "off", autocorrect: "off", autocapitalize: "off", spellcheck: "false") as String
                    mkp.yieldUnescaped('\n' + field + '\n')
                }
            }
        }
        mb.hr()
        out.write(pluginSelector(bean: bean.source, cssClass: cssClass, prefix: "${prefix}.source", name: 'Source', wrappers: storageWrappers, role: RoleType.Source) as String)
        mb.hr()
        out.write(pluginSelector(bean: bean.target, cssClass: cssClass, prefix: "${prefix}.target", name: 'Target', wrappers: storageWrappers, role: RoleType.Target) as String)
        mb.hr()
        mb.div(id: filterDivId) {
            div(class: 'active-filters') {
                mkp.yieldUnescaped(' ') // workaround for zero filters
                bean.filters.eachWithIndex { filter, idx ->
                    mkp.yieldUnescaped(pluginSelector(bean: filter, cssClass: cssClass, prefix: "${prefix}.filters[${idx}]", name: "Filter ${idx + 1}",
                            wrappers: filterWrappers, delete: true) as String)
                }
            }
            div(class: 'filter-template') {
                mkp.yieldUnescaped(pluginSelector(cssClass: cssClass, prefix: "${prefix}.filters[00]", name: 'Filter 00',
                        wrappers: filterWrappers, delete: true) as String)
            }
            script "\$(function() {\$(document.getElementById('${filterDivId}')).find('.filter-template').find(':input').attr('disabled','disabled');});"
            p {
                a(href: '#', onclick: "addFilter(document.getElementById('${filterDivId}')); return false") {
                    mkp.yieldUnescaped('add filter')
                }
            }
        }
        mb.div(class: 'panel panel-default', id: syncOptionsDivId) {
            div(class: 'panel-heading') {
                h3(class: 'panel-title') {
                    mkp.yieldUnescaped('Sync Options ')
                }
                mb.a(class: 'toggle-advanced', href: '#', onclick: "toggleAdvanced(document.getElementById('${syncOptionsDivId}')); return false;") {
                    mkp.yieldUnescaped('show advanced options')
                }
            }
            div(class: 'panel-body') {
                table(class: "table table-striped table-condensed kv-table ${cssClass ?: ''}") {
                    mkp.yieldUnescaped('\n' + plugin(bean: bean.options, cssClass: cssClass, prefix: "${prefix}.options", name: 'Options', wrapper: optionWrapper) + '\n')
                }
            }
        }
    }

    def pluginSelector = { attrs ->
        def bean = attrs.bean
        def cssClass = attrs.cssClass
        def prefix = attrs.prefix
        def name = attrs.name
        def wrappers = attrs.wrappers.findAll { it.role in [null, attrs.role] }
        def role = attrs.role
        def id = "_${prefix}_"
        def labels = [''] + wrappers.collect { it.label }
        def keys = [''] + wrappers.collect { it.targetClass.name }

        if (!wrappers) throwTagError("Tag [pluginSelector] is missing required attribute [wrappers]")

        def mb = new MarkupBuilder(out)
        mb.div(class: "panel panel-default ${cssClass ?: ''}", id: id) {
            div(class: 'panel-heading') {
                h3(class: 'panel-title') {
                    span() {
                        mkp.yieldUnescaped("${name}: ")
                    }
                    mkp.yieldUnescaped('\n' + select(name: "${prefix}.pluginClass", from: labels, keys: keys,
                            value: bean?.class?.name ?: bean?.pluginClass, onchange: "changePlugin('${name}', this)") + '\n')
                    span(class: 'plugin-info glyphicon glyphicon-info-sign', 'data-plugin': name)
                }
                a(class: 'toggle-advanced', href: '#', onclick: "toggleAdvanced(document.getElementById('${id}')); return false;") {
                    mkp.yieldUnescaped('show advanced options')
                }
                if (attrs.delete) {
                    span(class: 'delete btn btn-sm btn-danger', onclick: "deleteFilter(document.getElementById('${id}'))", title: 'Remove this filter') {
                        mkp.yieldUnescaped('X')
                    }
                }
            }
            div(class: 'panel-body') {
                table(class: "table table-striped table-condensed kv-table ${cssClass ?: ''}", 'data-plugin': name) {
                    wrappers.each { wrapper ->
                        mkp.yieldUnescaped('\n' + plugin(bean: wrapper.targetClass.name == (bean?.class?.name ?: bean?.pluginClass) ? bean : null,
                                cssClass: cssClass, prefix: prefix, wrapper: wrapper, role: role) + '\n')
                    }
                }
            }
        }
        mb.script "\$(function () {changePlugin('${name}', document.getElementById('${prefix}.pluginClass'));});"
    }

    def plugin = { attrs ->
        def bean = attrs.bean
        def cssClass = attrs.cssClass
        def prefix = attrs.prefix
        def wrapper = attrs.wrapper as ConfigWrapper
        def role = attrs.role
        def properties = wrapper.propertyNames().sort { a, b -> wrapper.getPropertyWrapper(a).orderIndex <=> wrapper.getPropertyWrapper(b).orderIndex }

        if (!bean && !wrapper) throwTagError("Tag [plugin] is missing a required attribute (one of [bean] or [wrapper])")
        if (!bean) bean = wrapper.targetClass.newInstance()
        if (!wrapper) wrapper = ConfigUtil.wrapperFor(bean.class)

        def mb = new MarkupBuilder(out)
        mb.tbody('data-plugin': wrapper.targetClass.name, 'data-info': wrapper.documentation) {
            properties.each {
                if (wrapper.getPropertyWrapper(it).role in [null, role]) {
                    mkp.yieldUnescaped('\n' + property(bean: bean."${it}", cssClass: cssClass, formName: "${prefix}.${it}",
                            wrapper: wrapper.getPropertyWrapper(it)) + '\n')
                }
            }
        }
    }

    def property = { attrs ->
        def bean = attrs.bean
        def cssClass = attrs.cssClass ?: ''
        def wrapper = attrs.wrapper as ConfigPropertyWrapper
        def formStruct = formStruct(wrapper)
        def formName = attrs.formName
        if (wrapper.advanced) cssClass += ' advanced'

        if (!wrapper) throwTagError("Tag [property] is missing a required attribute [wrapper]")

        def mb = new MarkupBuilder(out)
        mb.tr(class: cssClass) {
            th { mkp.yieldUnescaped(wrapper.label) }
            td {
                def input = ''
                def value = wrapper.descriptor.propertyType.isArray() ? bean?.join('\n') : bean
                if (formStruct.formType == FormType.Text && wrapper.sensitive) {
                    input = g.passwordField(name: formName, value: value, title: wrapper.description, required: wrapper.required ?: null,
                            size: formStruct.size, placeholder: formStruct.placeholder,
                            autocomplete: "off", autocorrect: "off", autocapitalize: "off", spellcheck: "false")
                } else if (formStruct.formType == FormType.Text && !wrapper.sensitive) {
                    input = g.textField(name: formName, value: value, title: wrapper.description, required: wrapper.required ?: null,
                            size: formStruct.size, placeholder: formStruct.placeholder,
                            autocomplete: "off", autocorrect: "off", autocapitalize: "off", spellcheck: "false")
                } else if (formStruct.formType == FormType.TextArea) {
                    input = g.textArea(name: formName, value: value, title: wrapper.description, required: wrapper.required ?: null,
                            cols: formStruct.size, placeholder: formStruct.placeholder,
                            autocomplete: "off", autocorrect: "off", autocapitalize: "off", spellcheck: "false")
                } else if (formStruct.formType == FormType.Checkbox) {
                    input = g.checkBox(name: formName, value: value, title: wrapper.description,
                            autocomplete: "off", autocorrect: "off", autocapitalize: "off", spellcheck: "false")
                } else if (formStruct.formType == FormType.Select) {
                    input = g.select(name: formName, value: value, title: wrapper.description, required: wrapper.required ?: null,
                            from: formStruct.values, keys: formStruct.values,
                            autocomplete: "off", autocorrect: "off", autocapitalize: "off", spellcheck: "false")
                }
                mkp.yieldUnescaped('\n' + (input as String) + '\n')
                // add password toggle
                if (wrapper.sensitive) {
                    span(class: 'passwordToggle', 'data-target-id': formName) { 'show' }
                }
            }
        }
    }

    static FormStruct formStruct(ConfigPropertyWrapper wrapper) {
        def formStruct = new FormStruct()

        def inferredSize = 10
        def inferredType = FormType.Text
        def values = []
        def propType = wrapper.descriptor.propertyType
        if (propType == String.class) {
            inferredSize = 60
        } else if (propType.isArray() && propType.componentType == String.class) {
            inferredType = FormType.TextArea
            inferredSize = 60
        } else if (propType == Boolean.class || propType == Boolean.TYPE) {
            inferredType = FormType.Checkbox
        } else if (propType == Integer.class || propType == Integer.TYPE || propType == Float.class || propType == Float.TYPE) {
        } else if (propType == Long.class || propType == Long.TYPE || propType == Double.class || propType == Double.TYPE) {
            inferredSize = 20
        } else if (propType.isEnum()) {
            inferredType = FormType.Select
            values = [''] // can't find a way to one-line this
            values += propType.getEnumConstants().collect { it }
        }

        formStruct.formType = wrapper.formType
        if (formStruct.formType == FormType.Infer) formStruct.formType = inferredType
        formStruct.size = inferredSize
        formStruct.placeholder = wrapper.valueHint ?: wrapper.cliName
        formStruct.values = wrapper.valueList
        if (formStruct.values == null || formStruct.values.size() == 0)
            formStruct.values = values.collect { it.toString() }

        formStruct
    }

    static class FormStruct {
        FormType formType
        int size
        String placeholder
        def values = []
    }
}
