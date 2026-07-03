-- admit_and_move.lua — atomic SSD admission + lease mint/refresh + queue move.
--
-- Called by dispatch_eligible() in transcodarr-entrypoint.sh when
-- TRANSCODARR_TMP_DIR is set.
--
-- KEYS[1] = source_queue           (tc:lb:*:ready, :import:ready, or :direct:ready)
-- KEYS[2] = dest_queue             (tc:dispatch:*:ready)
-- KEYS[3] = tc:ssd:free_kb         (current free bytes on tmp pool)
-- KEYS[4] = tc:ssd:warn_kb         (minimum reserved free threshold)
-- KEYS[5] = tc:ssd:leases          (index set of active lease keys)
-- KEYS[6] = tc:ssd:space_ready     (monitor liveness heartbeat, TTL = 2x interval)
--
-- ARGV[1] = item                   (exact string currently in source_queue)
-- ARGV[2] = job_kb                 (this reservation's size in KB)
-- ARGV[3] = lease_key              (tc:ssd:lease:<file_hash>:<nonce>, minted or existing)
-- ARGV[4] = ttl_sec                (lease key TTL, must be a positive integer)
-- ARGV[5] = reuse                  ("1" = refreshing an existing lease from an exit-75
--                                   retry bounce, "0" = fresh admission)
--
-- Returns:
--    1  admitted; item moved to dest_queue, lease SETEX'd, index SADD'd
--    0  rejected (no space, missing liveness heartbeat, or invalid ttl)
--   -1  race condition — item was LREM'd from source by someone else between
--       LINDEX and this EVAL; caller should try the next item in the loop

local job = tonumber(ARGV[2])
local ttl = tonumber(ARGV[4])
local reuse = ARGV[5]

-- TTL must be a positive integer, or SETEX will error AFTER LREM mutates the
-- source queue and the item would be lost. Redis/Valkey does not roll back
-- prior writes on runtime error inside a Lua script — fail closed.
if ttl == nil or ttl <= 0 then
  return 0
end

-- Liveness gate: require space_ready heartbeat. Missing = space_monitor
-- stalled, died, or valkey flushed. Fail closed.
if redis.call("EXISTS", KEYS[6]) == 0 then
  return 0
end

local free_raw = redis.call("GET", KEYS[3])
local warn_raw = redis.call("GET", KEYS[4])
-- With space_ready present, free/warn should also be present (they are
-- seeded synchronously before LB starts). If they somehow are missing here,
-- treat as fail-closed rather than fail-open.
if free_raw == false or warn_raw == false then
  return 0
end
local free_kb = tonumber(free_raw) or 0
local warn_kb = tonumber(warn_raw) or 0

-- Sum live leases, self-heal stale index entries. On reuse, exclude our own
-- lease from the sum so the retry is not rejected by its own in-flight charge.
local sum = 0
local members = redis.call("SMEMBERS", KEYS[5])
for _, k in ipairs(members) do
  local v = redis.call("GET", k)
  if v then
    if not (reuse == "1" and k == ARGV[3]) then
      sum = sum + tonumber(v)
    end
  else
    redis.call("SREM", KEYS[5], k)
  end
end

local effective = free_kb - sum
if effective < 0 then effective = 0 end
if effective < (job + warn_kb) then
  return 0
end

local removed = redis.call("LREM", KEYS[1], 1, ARGV[1])
if removed < 1 then return -1 end

-- Mint or refresh the lease. SETEX is idempotent — on reuse it just resets
-- the TTL on the existing key. SADD is idempotent too, so we always call it;
-- harmless no-op on reuse.
redis.call("SETEX", ARGV[3], ttl, ARGV[2])
redis.call("SADD", KEYS[5], ARGV[3])

-- LPUSH the item. On reuse the item string already carries the lease key
-- at field 12, so push it unchanged. On fresh admission, append the
-- newly-minted lease key as a new 12th field.
if reuse == "1" then
  redis.call("LPUSH", KEYS[2], ARGV[1])
else
  redis.call("LPUSH", KEYS[2], ARGV[1] .. "|" .. ARGV[3])
end
return 1
