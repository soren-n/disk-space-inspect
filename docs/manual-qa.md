# Manual QA Checklist

Use this list before releases to sanity-check common workflows:

1. **Cold Scan**
   - Launch `dusk` pointed at a moderately sized directory.
   - Confirm the tree populates incrementally and the status bar numbers match expectations.
2. **Incremental Updates**
   - Modify, add, and delete files under the active root.
   - Ensure the watcher toggled on causes "Watcher detected changes" messaging and triggers a follow-up scan.
3. **Cache Clear**
   - Use the context menu to clear the cache.
   - Verify a fresh scan runs automatically and the cache stats reset.
4. **Layout Reset**
   - Tweak column widths via `Layout` → `Layout Settings` and then reset to defaults.
   - Restart the app to confirm the persisted widths and sort order survive reloads.
5. **Deletion Workflow**
   - Stage multiple entries, open the confirmation dialog, and complete a delete.
   - Confirm the cache marks ancestors dirty and the subsequent scan reflects removals.
