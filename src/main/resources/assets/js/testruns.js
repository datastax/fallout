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

function toggleKeepTestRunForever(userEmail, testName, testRunId, shortTestRunId) {
    $.ajax({
            url: "/tests/" + userEmail + "/" + testName +
                "/runs/" + testRunId + "/toggleKeepForever/api",
            type: "POST",
            success: function(data) {
                location.reload();
            },
            error: function(data) {
                showErrorMessage("There was an error toggling keep forever Test Run:<br>" +
                    data.responseText);
            }
        });
}
