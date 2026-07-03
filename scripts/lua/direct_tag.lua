-- direct_tag.lua — atomic tag move from bulk/import to direct lane.
--
-- Called by POST /api/direct/tag. Removes the item from its current
-- source lane, stamps the original lane name into sidecar metadata
-- for untag restore, and pushes the rewritten payload (with field 11
-- flipped to "direct") into the direct lane.
--
-- All or nothing: if LREM finds zero items, nothing else runs.
--
-- KEYS[1] = source_lane          (tc:lb:*:ready or tc:lb:*:import:ready)
-- KEYS[2] = direct_lane          (tc:lb:*:direct:ready — gpu or cpu to match source)
-- KEYS[3] = meta_key             (tc:direct:meta:<file_hash>)
-- ARGV[1] = current_payload      (exact string currently in source_lane)
-- ARGV[2] = rewritten_payload    (same payload with field 11 = "direct")
-- ARGV[3] = original_lane        (duplicate of KEYS[1] — persisted for untag)
--
-- Returns:
--    1  tagged successfully
--    0  item not found in source_lane (race or stale client state)

local removed = redis.call("LREM", KEYS[1], 1, ARGV[1])
if removed == 0 then return 0 end
redis.call("SET", KEYS[3], ARGV[3])
redis.call("LPUSH", KEYS[2], ARGV[2])
return 1
