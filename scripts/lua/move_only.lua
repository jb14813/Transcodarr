-- move_only.lua — plain atomic LREM+LPUSH for the non-TMP_DIR path.
--
-- Called by dispatch_eligible() in transcodarr-entrypoint.sh when
-- TRANSCODARR_TMP_DIR is unset. No lease accounting, no space check —
-- same semantics as the original pre-lease move EVAL.
--
-- KEYS[1] = source_queue           (tc:lb:*:ready, :import:ready, or :direct:ready)
-- KEYS[2] = dest_queue             (tc:dispatch:*:ready)
-- ARGV[1] = item                   (exact string currently in source_queue)
--
-- Returns:
--    1  moved
--   -1  raced (item already gone from source)

local removed = redis.call("LREM", KEYS[1], 1, ARGV[1])
if removed < 1 then return -1 end
redis.call("LPUSH", KEYS[2], ARGV[1])
return 1
