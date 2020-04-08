/* Copyright (c) 2012: Daniel Richman. License: GNU GPL 3 */
/* Additional features: Priyesh Patel                     */

function init_logtail(url, serverSentEventsUrl, maxShownKB, dataelem) {
    var load = maxShownKB * 1024; /* 30KB */

    var kill = false;
    var loading = false;
    var tail = true;
    var log_data = "";
    var log_file_size = 0;

    /* :-( https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/parseInt */
    function parseInt2(value) {
        if (!(/^[0-9]+$/.test(value))) throw "Invalid integer " + value;
        var v = Number(value);
        if (isNaN(v)) throw "Invalid integer " + value;
        return v;
    }

    function assertNotNull(value, valueName) {
        if (value === null) {
            throw valueName + " is not set";
        }
        return value;
    }

    function getRequiredResponseHeader(xhr, header) {
        return assertNotNull(xhr.getResponseHeader(header), "Response Header " + header)
    }

    function get_log() {
        if (kill | loading) return;
        loading = true;

        var range;
        var first_load;
        var must_get_206;
        if (log_file_size === 0) {
            /* Get the last 'load' bytes */
            range = "-" + load.toString();
            first_load = true;
            must_get_206 = false;
        } else {
            /* Get the (log_file_size - 1)th byte, onwards. */
            range = (log_file_size - 1).toString() + "-";
            first_load = false;
            must_get_206 = log_file_size > 1;
        }

        /* The "log_file_size - 1" deliberately reloads the last byte, which we already
         * have. This is to prevent a 416 "Range unsatisfiable" error: a response
         * of length 1 tells us that the file hasn't changed yet. A 416 shows that
         * the file has been trucnated */

        $.ajax(url, {
            dataType: "text",
            cache: false,
            headers: {Range: "bytes=" + range},
            success: function (data, s, xhr) {
                loading = false;

                var content_size;

                if (xhr.status === 206) {
                    var c_r = getRequiredResponseHeader(xhr, "Content-Range");
                    log_file_size = parseInt2(assertNotNull(c_r.split("/")[1], "Content-Range size"));
                    content_size = getRequiredResponseHeader(xhr, "Content-Length");
                } else if (xhr.status === 200) {
                    if (must_get_206)
                        throw "Expected 206 Partial Content";

                    content_size = log_file_size = getRequiredResponseHeader(xhr, "Content-Length");
                } else {
                    throw "Unexpected status " + xhr.status;
                }

                if (first_load && data.length > load)
                    throw "Server's response was too long";

                var added = false;

                if (first_load) {
                    /* Clip leading part-line if not the whole file */
                    if (content_size < log_file_size) {
                        var start = data.indexOf("\n");
                        log_data = data.substring(start + 1);
                    } else {
                        log_data = data;
                    }

                    added = true;
                } else {
                    /* Drop the first byte (see above) */
                    log_data += data.substring(1);

                    if (log_data.length > load) {
                        var start = log_data.indexOf("\n", log_data.length - load);
                        log_data = log_data.substring(start + 1);
                    }

                    if (data.length > 1)
                        added = true;
                }

                if (added)
                    show_log(added);
            },
            error: function (xhr, s, t) {
                loading = false;

                if (xhr.status === 416 || xhr.status === 404) {
                    /* 416: Requested range not satisfiable: log was truncated. */
                    /* 404: Retry soon, I guess */

                    log_file_size = 0;
                    log_data = "";
                    show_log();

                } else {
                    throw "Unknown AJAX Error (status " + xhr.status + ")";
                }
            }
        });
    }

    function scroll(where) {
        // html works on chrome, body on safari.  Do both, just to be safe.
        let s = $("html,body");
        if (where === -1) {
            s.scrollTop(s.height());
        }
        else {
            s.scrollTop(where);
        }
    }

    function show_log() {
        // Scroll-to-bottom only if we're already looking at the bottom of
        // the page
        let nearlyAtBottomFudgeDistance = 20;
        let atBottom = window.innerHeight + window.pageYOffset + nearlyAtBottomFudgeDistance >=
            document.body.offsetHeight;

        $(dataelem).text(log_data);

        if (atBottom) {
            scroll(-1);
        }
    }

    function error(what) {
        kill = true;

        $(dataelem).text("An error occured:\r\n" +
            what + "\r\n" +
            "Reloading may help; if it doesn't, please report it in #fallout");
        scroll(0);

        return false;
    }

    $(document).ready(function () {
        window.onerror = error;

        let eventSource = new EventSource(serverSentEventsUrl);

        eventSource.addEventListener("state", e => {
            switch (e.data) {
                case "updated":
                    get_log();
                    break;
                case "finished":
                    eventSource.close();
                    break;
            }
        });

        get_log();
    });

}
