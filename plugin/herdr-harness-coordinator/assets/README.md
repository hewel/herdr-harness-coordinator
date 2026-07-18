# Harness Coordinator plugin assets

This directory is reserved for static popup assets. The MVP popup is rendered
by the `herdr-harness-coordinator popup` process and queries durable Coordinator
state; it does not screen-scrape panes or own any Harness lifecycle.

The `worker` entrypoint inherits `HERDR_SOCKET_PATH` from Herdr and receives
`HERDR_COORDINATOR_STATE_DIR` and `HERDR_HARNESS_SESSION_ID` from
`plugin.pane.open`. `HERDR_PLUGIN_STATE_DIR` remains the Herdr-owned plugin root;
it is not used as a per-workspace state directory.
