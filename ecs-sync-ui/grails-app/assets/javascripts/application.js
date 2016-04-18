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

var showAdvanced = false;

function toggleAdvanced(link) {
    if (showAdvanced) {
        $('.advanced').hide();
        link.text('show advanced options');
        showAdvanced = false;
    } else {
        $('.advanced').show();
        link.text('hide advanced options');
        showAdvanced = true;
    }
}

function changePlugin(type, selectField) {
    var pluginName = $(selectField).val().split('.').pop();
    var $allsections = $("table[data-plugin='" + type + "'] tbody");
    $allsections.hide();
    $allsections.find('input, textarea, button').prop('disabled', 'disabled');
    var $validsection = $("table[data-plugin='" + type + "'] tbody[data-plugin='" + pluginName + "']");
    $validsection.find('input, textarea, button').prop('disabled', false);
    $validsection.show();
}