# Deleting Tests

Tests and test runs are stored in the database, whereas artifacts are stored in the filesystem. For any orphaned artifacts, a clean up job is run every 24 hours to check all artifacts for a matching test run in the database. If there is no match, the artifacts are deleted. 

# Orphaned Artifacts

When tests and test runs are deleted in Fallout, they are first marked as deleted and remain for 30 days before being deleted. The test runs' artifacts are not always deleted along with the test run leaving them orphaned. To ensure these artifacts are deleted a clean-up job is run every 24 hours to find artifacts with no matching test database entry and delete them.
