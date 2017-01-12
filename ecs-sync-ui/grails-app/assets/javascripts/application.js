// This is a manifest file that'll be compiled into application.js.
//
// Any JavaScript file within this directory can be referenced here using a relative path.
//
// You're free to add application-wide JavaScript to this file, but it's generally better
// to create separate JavaScript files as needed.
//
//= require jquery-2.1.3.js
//= require bootstrap-all
//= require_tree .
//= require_self

if (typeof jQuery !== 'undefined') {
    (function($) {
        $('#spinner').ajaxStart(function() {
            $(this).fadeIn();
        }).ajaxStop(function() {
            $(this).fadeOut();
        });
    })(jQuery);
}

$(document).ready(function() {
    $('.disabled').prop('disabled', true);

    $('.passwordToggle').click(function() {
        var $this = $(this);
        var $target = $('#' + $this.data('targetId'));
        var isPassword = $target.attr('type') == 'password';
        if (isPassword) {
            $target[0].type = 'text';
            $this.text('hide');
        } else {
            $target[0].type = 'password';
            $this.text('show');
        }
    });
});

function toggleAdvanced(container) {
    var $container = $(container);
    var $link = $container.find('a.toggle-advanced');
    var advancedShowing = $link.text() == 'hide advanced options';

    if (advancedShowing) {
        $container.find('.advanced').hide();
        $link.text('show advanced options');
    } else {
        $container.find('.advanced').show();
        $link.text('hide advanced options');
    }
    //container.scrollIntoView(true);
}

function changePlugin(propertyName, selectField) {
    var pluginName = $(selectField).val();
    var $allsections = $("table[data-plugin='" + propertyName + "'] tbody");
    $allsections.hide();
    $allsections.find(':input').prop('disabled', 'disabled');
    var $validsection = $("table[data-plugin='" + propertyName + "'] tbody[data-plugin='" + pluginName + "']");
    $validsection.find(':input').prop('disabled', false);
    $validsection.show();
}

function changeConfigStorage(radioField) {
    var storageType = $(radioField).val();
    var $allsections = $("table#storage-configuration tbody");
    $allsections.hide();
    $allsections.find(':input').prop('disabled', 'disabled');
    var $validsection = $("table#storage-configuration tbody[data-storage-type='" + storageType + "']");
    $validsection.find(':input').prop('disabled', false);
    $validsection.show();
}

function addFilter(filterContainerDiv) {
    var $newFilter = $(filterContainerDiv).find('.filter-template div.panel').clone();
    $newFilter.find('.panel-title :input').attr('disabled', false);
    $(filterContainerDiv).find('.active-filters').append($newFilter);
    reOrderFilters(filterContainerDiv);
}

function deleteFilter(filterDiv) {
    var filterContainerDiv = filterDiv.parentElement.parentElement;
    $(filterDiv).remove();
    reOrderFilters(filterContainerDiv);
}

function reOrderFilters(filterContainerDiv) {
    $(filterContainerDiv).find('.active-filters div.panel').each(function (index) {
        var id = $(this).attr('id');
        id = id.replace(/\.filters\[[0-9]*]/g, '.filters[' + index + ']');
        $(this).attr('id', id);

        $(this).find('.panel-title *:contains("Filter ")').each(function () {
            $(this).text($(this).text().replace(/Filter [0-9]*/g, 'Filter ' + (index + 1)));
        });
        $(this).find('[name]').each(function () {
            $(this).attr('name', $(this).attr('name').replace(/\.filters\[[0-9]*]/g, '.filters[' + index + ']'));
        });
        $(this).find('[id]').each(function () {
            $(this).attr('id', $(this).attr('id').replace(/\.filters\[[0-9]*]/g, '.filters[' + index + ']'));
        });
        $(this).find('[onclick]').each(function () {
            $(this).attr('onclick', $(this).attr('onclick').replace(/\.filters\[[0-9]*]/g, '.filters[' + index + ']'));
        });
        $(this).find('[onchange]').each(function () {
            $(this).attr('onchange', $(this).attr('onchange').replace(/Filter [0-9]*/g, 'Filter ' + (index + 1)));
        });
        $(this).find("[data-plugin]").each(function () {
            $(this).attr('data-plugin', $(this).attr('data-plugin').replace(/Filter [0-9]*/g, 'Filter ' + (index + 1)));
        });
    });
}
