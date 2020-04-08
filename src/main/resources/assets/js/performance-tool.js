var groupCount = 0;
var allUsers = null;

function initiate(defaultUser) {
    $.ajax({
        url: "/account/users/api",
        type: "GET",
        success: function (data) {
            allUsers = data;
            allUsers = allUsers.sort(function(u1, u2){
                                         if (u1.name < u2.name) {
                                            return -1;
                                         }
                                         else if (u1.name > u2.name) {
                                            return 1;
                                         }
                                         return 0;
                                     });
            buildDefaultView(defaultUser);
        },
        error: function (data) {
            showErrorMessage("Error getting users:\n" + data.responseText);
        }
    });
}

function buildDefaultView(defaultUser) {
    addTestGroup(defaultUser);
    getTestsAndUpdateSelector({"data": groupCount});
}

function addTestGroup(defaultUser) {
    groupCount += 1;
    var groupId = groupCount;
    var userSelector = createUserSelectorHtml(defaultUser, groupId);
    var userEmail = defaultUser['email'];

    var testGroupHtml = '\
        <div id="test-group-' + groupId + '" class="col-lg-12">\
            <div class="form-group col-lg-4">\
                <label>Choose User #' + groupId + '</label>\
                ' + userSelector + '\
            </div>\
            \
            <div class="form-group col-lg-4">\
                <label>Choose Test #' + groupId + '</label>\
                <select id="test-' + groupId + '" class="form-control" name="test-name-' + groupId + '" disabled>\
                    <option value="">Select User First</option>\
                </select>\
            </div>\
            \
            <div class="form-group col-lg-4">\
                <label>Choose Test Run #' + groupId + '</label>\
                <select id="testrun-' + groupId + '" class="form-control" name="test-run-' + groupId + '" disabled>\
                    <option value="">Select Test First</option>\
                </select>\
            </div>\
        </div>\
    ';

    $("#test-components").append(testGroupHtml);
    $("#user-" + groupId).change(groupId, getTestsAndUpdateSelector);

    getTestsAndUpdateSelectorForGroupIdAndUserEmail(groupId, userEmail);
}

function createUserSelectorHtml(defaultUser, groupId) {
    // Make a copy of allUsers because we modify the array
    var users = allUsers.slice()

    var html = '<select class="form-control" name="user-' + groupId + '" id="user-' + groupId + '">';

    var userSelectorOptions = "";

    if (defaultUser !== null) {
        // remove default user from users array, if found
        for (var i = 0; i < users.length; i++) {
            if (users[i]['email'] === defaultUser['email'] && users[i]['name'] === defaultUser['name']) {
                users.splice(i, 1);
                break;
            }
        }

        userSelectorOptions += '<option selected value="' + defaultUser['email'] + '">' + defaultUser['name'] + ' (' + defaultUser['email'] + ')</option>';
    }
    else {
        userSelectorOptions += '<option selected value=""></option>';
    }

    for (var i = 0; i < users.length; i++) {
        userSelectorOptions += '<option value="' + users[i]['email'] + '">' + users[i]['name'] + ' (' + users[i]['email'] + ')</option>';
    }

    html += userSelectorOptions;
    html += '</select>';

    return html;
}

function getTestsAndUpdateSelector(eventData) {
    var groupId = eventData['data'];
    var userEmail = $("#user-" + groupId).val();

    getTestsAndUpdateSelectorForGroupIdAndUserEmail(groupId, userEmail);
}

function getTestsAndUpdateSelectorForGroupIdAndUserEmail(groupId, userEmail) {
    // Clear previous html
    $("#test-" + groupId).html("");
    $("#testrun-" + groupId).html("");

    $.ajax({
        url: "/tests/" + userEmail + "/api",
        type: "GET",
        success: function (data) {
            updateTestSelector(data);
        },
        error: function (data) {
            if (data.status === 404) {
                showErrorMessage("Error getting tests. The user probably doesn't have any.")
            }
            else {
                showErrorMessage("Error getting tests:\n" + data.responseJSON['error']);
            }
        }
    });

    function getPreviousGroupTest(groupId) {
        if (groupId === 1) {
            return null;
        }
        else {
            var e = $("#test-" + (groupId - 1))[0];
            var selectedTest = e.options[e.selectedIndex].value;
            if (selectedTest) {
                return selectedTest;
            }
            else {
                return null;
            }
        }
    }

    function updateTestSelector(tests) {
        var testOptionsHtml = '<option selected value=""></option>';
        var previousGroupTest = getPreviousGroupTest(groupId);

        for (var i = 0; i < tests.length; i++) {
            if (tests[i]['name'] === previousGroupTest) {
                testOptionsHtml += '<option selected value="' + tests[i]['name'] + '">' + tests[i]['name'] + '</option>';
            }
            else {
                testOptionsHtml += '<option value="' + tests[i]['name'] + '">' + tests[i]['name'] + '</option>';
            }
        }
        $("#test-" + groupId).html(testOptionsHtml);
        $("#test-" + groupId).removeAttr("disabled");

        $("#test-" + groupId).change(getTestRunsAndUpdateSelector)

        if (previousGroupTest !== null) {
            getTestRunsAndUpdateSelector();
        }
    }

    function getTestRunsAndUpdateSelector() {
        var selectedTestName = $("#test-" + groupId).val();
        var userEmail = $("#user-" + groupId).val();

        $.ajax({
            url: "/tests/" + userEmail + "/" + selectedTestName + "/runs/simple/api",
            type: "GET",
            success: function (data) {
                updateTestRunSelector(data);
            },
            error: function (data) {
                showErrorMessage("Error getting test run:\n" + data.responseJSON['error']);
            }
        });

        function updateTestRunSelector(testRuns) {
            var testRunOptionsHtml = '<option selected value=""></option>';

            for (var i = 0; i < testRuns.length; i++) {
                testRunOptionsHtml += '<option value="' + testRuns[i]['testRunId'] + '">' + testRuns[i]['displayName'] + '</option>';
            }
            $("#testrun-" + groupId).html(testRunOptionsHtml);
            $("#testrun-" + groupId).removeAttr("disabled");
        }
    }
}

function submitFormToBuild(userEmail) {

    showInfoMessage("Building performance report. This may take some time.");

    $.ajax({
        url: "/performance/run",
        type: "POST",
        data: $('#select-runs-form').serialize(),
        success: function (data) {
            //redirect
            window.location = "/performance/" + userEmail;
        },
        error: function (data) {
            $(".form-control").prop('disabled', false);
            $("#buildPerfReport, #addGroup").prop('disabled', false);
            showErrorMessage(data.responseJSON['error']);
        }
    });

    $(".form-control").prop('disabled', true);
    $("#buildPerfReport, #addGroup").prop('disabled', true);
}
