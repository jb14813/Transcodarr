-- direct_untag.lua — atomic untag restore from direct lane.
--
-- Called by POST /api/direct/untag. Removes the item from the direct
-- lane, pushes the rewritten payload (with field 11 flipped back to
-- "bulk" or "import") into the restore lane, and deletes the sidecar
-- metadata key.
--
-- The caller must have already validated:
--   - direct_lane is in the allow-list
--   - restore_lane (from GET meta) is in the allow-list
--   - If meta was missing or invalid, caller returns meta_missing
--     without calling this script.
--
-- KEYS[1] = direct_lane          (tc:lb:*:direct:ready)
-- KEYS[2] = restore_lane         (from GET meta — bulk or import lane)
-- KEYS[3] = meta_key             (tc:direct:meta:<file_hash>)
-- ARGV[1] = current_payload      (exact string currently in direct_lane, field 11 = "direct")
-- ARGV[2] = rewritten_payload    (field 11 reset to original_route matching restore_lane)
--
-- Returns:
--    1  untagged successfully
--    0  item not found in direct_lane (race or stale client state)

local removed = redis.call("LREM", KEYS[1], 1, ARGV[1])
if removed == 0 then return 0 end
redis.call("LPUSH", KEYS[2], ARGV[2])
redis.call("DEL", KEYS[3])
return 1
