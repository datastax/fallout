function rerunTestRun(userEmail, testName, testRunId, clone) {
    $.ajax({
        url: "/tests/" + userEmail + "/" + testName +
            "/runs/" + testRunId + "/rerun/api",
        type: "POST",
        contentType: "application/json",
        data: JSON.stringify({
            clone: clone
        }),
        success: function(data) {
            location.reload();
        },
        error: function(data) {
            showErrorMessage("There was an error rerunning the Test Run:<br>" +
                data.responseText);
        }
    });
}

function abortTestRun(userEmail, testName, testRunId) {
    $.ajax({
        url: "/tests/" + userEmail + "/" + testName +
            "/runs/" + testRunId + "/abort/api",
        type: "POST",
        success: function(data) {
            location.reload();
        },
        error: function(data) {
            showErrorMessage("There was an error aborting the Test Run:<br>" +
                data.responseText);
        }
    });
}

function deleteTestRun(userEmail, testName, testRunId, shortTestRunId) {
    if (!confirm("Delete test run " + shortTestRunId + "?"))
        return;

    $.ajax({
        url: "/tests/" + userEmail + "/" + testName +
            "/runs/" + testRunId + "/api",
        type: "DELETE",
        success: function(data) {
            location.reload();
        },
        error: function(data) {
            showErrorMessage("There was an error deleting the Test Run:<br>" +
                data.responseText);
        }
    });
}

function setKeepTestRunForever(userEmail, testName, testRunId, keepForever) {
    $.ajax({
            url: "/tests/" + userEmail + "/" + testName +
                "/runs/" + testRunId + "/setKeepForever/api" + "?keepForever=" + keepForever,
            type: "POST",
            success: function(data) {
                location.reload();
            },
            error: function(data) {
                showErrorMessage("There was an error setting keep forever Test Run:<br>" +
                    data.responseText);
            }
        });
}

function restoreTestRun(userEmail, testName, testRunId) {
    $.ajax({
        url: "/tests/deleted/" + userEmail + "/" + testName + "/runs/" + testRunId + "/restore/api",
        type: "POST",

        success: function(data){
            location.reload();
        },
        error: function(data){
            showErrorMessage("There is a problem restoring testRun: <br>" + data.responseText)
        }
    });
}
