(function() {
    let _showMessage = function (id, message) {
        $(id).html(message).show();
    };

    this.clearMessages = function() {
        $("#action-error, #action-success, #action-info").html("").hide();
    };

    this.showErrorMessage = function(message) {
        clearMessages();
        _showMessage("#action-error", message);
    };

    this.appendErrorMessage = function(message) {
        _showMessage("#action-error", $("#action-error").html() + message);
    };

    // .text() escapes the input, which is then retrieved using .html()
    let escapedHtml = text => $("<p/>").text(text).html();

    let escapedHtmlLines = text =>
        text.split("\n").map(escapedHtml).join("<br/>");

    this.showAjaxErrorMessage = function(data) {
        let container = $("#action-error").empty();

        if (data.responseJSON != null && data.responseJSON.errors != null) {
            let errors = $("<ul/>")
            for (var i = 0; i < data.responseJSON.errors.length; i++)
            {
                errors.append($("<li/>").text(data.responseJSON.errors[i]))
            }
            container.append(errors);
        }

        // io.dropwizard.jersey.errors.ErrorMessage
        else if (data.responseJSON != null && data.responseJSON.message != null) {
            container.append($("<h5/>")
                .html(escapedHtmlLines(data.responseJSON.message)));

            if (data.responseJSON.details != null) {
                container.append($("<p/>")
                    .html(escapedHtmlLines(data.responseJSON.details)));
            }

            if (data.responseJSON.code != null) {
                container.append($("<h5/>")
                    .text("HTTP code " + data.responseJSON.code));
            }
        }

        else {
            container.append($("<pre/>").text(JSON.stringify(data, null, 2)));
        }

        container.show();
    };

    this.showMessage = function(message) {
        clearMessages();
        _showMessage("#action-success", message);
        setTimeout(function(){
            $("#action-success").hide();
        }, 10000);
    };

    this.showInfoMessage = function(message) {
        clearMessages();
        _showMessage("#action-info", message);
    };
})();
