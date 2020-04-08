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
    this.showAjaxErrorMessage = function(data) {
        if (data.responseJSON != null && data.responseJSON.errors != null) {
            var fullErrorString = "<ul>";
            for (var i = 0; i < data.responseJSON.errors.length; i++)
            {
                fullErrorString += "<li>" + data.responseJSON.errors[i] + "</li>";
            }
            fullErrorString.concat("</ul>");
            showErrorMessage(fullErrorString);
        }

        // io.dropwizard.jersey.errors.ErrorMessage
        else if (data.responseJSON != null && data.responseJSON.message != null) {
            var message = "<h5>" + data.responseJSON.message + "</h5>";
            if (data.responseJSON.details != null) {
                message = message + "<p>" + data.responseJSON.details;
            }
            if (data.responseJSON.code != null) {
                message = message + "<h5>HTTP code " + data.responseJSON.code + "</h5>";
            }
            showErrorMessage(message.replace(/\n/g, "</br>"));
        }

        else {
            showErrorMessage("<pre>" + JSON.stringify(data, null, 2) + "</pre>");
        }
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
