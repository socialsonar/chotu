export const FAIR_POP_SCRIPT = `
local rotationKey = KEYS[1]
local workflowsKey = KEYS[2]
local inflightKey = KEYS[3]
local listPrefix = ARGV[1]
local rotations = redis.call('LLEN', rotationKey)
if rotations == 0 then return nil end
for i = 1, rotations do
  local workflowId = redis.call('RPOPLPUSH', rotationKey, rotationKey)
  local listKey = listPrefix .. workflowId
  local item = redis.call('RPOPLPUSH', listKey, inflightKey)
  if item then return item end
  if redis.call('LLEN', listKey) == 0 then
    redis.call('LREM', rotationKey, 0, workflowId)
    redis.call('SREM', workflowsKey, workflowId)
  end
end
return nil
`;

export const FAIR_ENQUEUE_SCRIPT = `
local wfListKey = KEYS[1]
local workflowsKey = KEYS[2]
local rotationKey = KEYS[3]
local stepKey = KEYS[4]
local stepExecId = ARGV[1]
local workflowRunId = ARGV[2]
local status = redis.call('HGET', stepKey, 'status')
if status ~= 'pending' then return 0 end
local queued = redis.call('HGET', stepKey, 'queued')
if queued == '1' then return 0 end
redis.call('HSET', stepKey, 'queued', '1')
redis.call('LPUSH', wfListKey, stepExecId)
if redis.call('SADD', workflowsKey, workflowRunId) == 1 then
  redis.call('RPUSH', rotationKey, workflowRunId)
end
return 1
`;

export const REQUEUE_INFLIGHT_SCRIPT = `
local inflightKey = KEYS[1]
local wfListKey = KEYS[2]
local workflowsKey = KEYS[3]
local rotationKey = KEYS[4]
local stepKey = KEYS[5]
local stepExecId = ARGV[1]
local workflowRunId = ARGV[2]
local status = redis.call('HGET', stepKey, 'status')
redis.call('LREM', inflightKey, 1, stepExecId)
if status ~= 'pending' then return 0 end
redis.call('HSET', stepKey, 'queued', '1')
redis.call('LPUSH', wfListKey, stepExecId)
if redis.call('SADD', workflowsKey, workflowRunId) == 1 then
  redis.call('RPUSH', rotationKey, workflowRunId)
end
return 1
`;

export const ACK_INFLIGHT_SCRIPT = `
local inflightKey = KEYS[1]
local stepKey = KEYS[2]
local stepExecId = ARGV[1]
redis.call('LREM', inflightKey, 1, stepExecId)
local status = redis.call('HGET', stepKey, 'status')
if status == 'pending' or status == 'completed' or status == 'failed' or status == 'cancelled' or status == 'waiting' then
  redis.call('HSET', stepKey, 'queued', '0')
end
return 1
`;

export const RATE_LIMIT_SCRIPT = `
local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
if count > tonumber(ARGV[2]) then
  redis.call('DECR', KEYS[1])
  return 0
end
return 1
`;

export const CLAIM_STEP_SCRIPT = `
local stepKey = KEYS[1]
local streamKey = KEYS[2]
local status = redis.call('HGET', stepKey, 'status')
if status ~= 'pending' then return nil end
local now = ARGV[1]
local leaseOwner = ARGV[2]
local leaseUntil = ARGV[3]
local stepExecId = ARGV[4]
redis.call('HSET', stepKey, 'status', 'running', 'updated_at', now, 'lease_owner', leaseOwner, 'lease_until', leaseUntil)
local version = redis.call('HINCRBY', stepKey, 'version', 1)
redis.call('XADD', streamKey, '*', 'type', 'step.status', 'id', stepExecId, 'status', 'running', 'updated_at', now, 'version', tostring(version))
return redis.call('HGETALL', stepKey)
`;

export const RENEW_LEASE_SCRIPT = `
local stepKey = KEYS[1]
local leaseOwner = ARGV[1]
local leaseUntil = ARGV[2]
local now = ARGV[3]
if redis.call('HGET', stepKey, 'lease_owner') ~= leaseOwner then return 0 end
if redis.call('HGET', stepKey, 'status') ~= 'running' then return 0 end
redis.call('HSET', stepKey, 'lease_until', leaseUntil, 'updated_at', now)
return 1
`;

export const RESET_EXPIRED_LEASE_SCRIPT = `
local stepKey = KEYS[1]
local streamKey = KEYS[2]
local nowMs = tonumber(ARGV[1])
local now = ARGV[2]
local stepId = ARGV[3]
local status = redis.call('HGET', stepKey, 'status')
if status ~= 'running' then return 0 end
local leaseUntil = redis.call('HGET', stepKey, 'lease_until')
if leaseUntil and leaseUntil ~= '' and tonumber(leaseUntil) > nowMs then return 0 end
redis.call('HSET', stepKey, 'status', 'pending', 'updated_at', now, 'lease_owner', '', 'lease_until', '0', 'queued', '0')
local version = redis.call('HINCRBY', stepKey, 'version', 1)
redis.call('XADD', streamKey, '*', 'type', 'step.status', 'id', stepId, 'status', 'pending', 'updated_at', now, 'version', tostring(version))
return 1
`;

export const CREATE_STEP_SCRIPT = `
local stepKey = KEYS[1]
local runKey = KEYS[2]
local activeKey = KEYS[3]
local branchesKey = KEYS[4]
local status = ARGV[5]
local isActive = (status == 'pending' or status == 'running' or status == 'waiting')
if isActive then
  local fanOutIndex = ARGV[8]
  local isFanOut = fanOutIndex ~= '' and fanOutIndex ~= 'null'
  if not isFanOut and redis.call('SCARD', activeKey) > 0 then return 0 end
  redis.call('SADD', activeKey, ARGV[1])
  redis.call('HINCRBY', runKey, 'active_count', 1)
end
redis.call('HSET', stepKey,
  'id', ARGV[1],
  'workflow_run_id', ARGV[2],
  'step_name', ARGV[3],
  'queue', ARGV[4],
  'status', status,
  'input', ARGV[6],
  'output', '',
  'error', '',
  'join_step_id', ARGV[7],
  'fan_out_index', ARGV[8],
  'join_total', ARGV[9],
  'join_remaining', ARGV[10],
  'attempts', ARGV[11],
  'queued', '0',
  'lease_owner', '',
  'lease_until', '0',
  'created_at', ARGV[12],
  'updated_at', ARGV[13],
  'version', '0'
)
local joinStepId = ARGV[7]
if joinStepId ~= '' and joinStepId ~= 'null' then
  redis.call('RPUSH', branchesKey, ARGV[1])
end
return 1
`;

export const ROLLBACK_STEP_SCRIPT = `
local stepKey = KEYS[1]
local runKey = KEYS[2]
local activeKey = KEYS[3]
local stepId = ARGV[1]
local status = redis.call('HGET', stepKey, 'status')
if not status then return 0 end
local active = { pending=1, running=1, waiting=1 }
if active[status] then
  redis.call('SREM', activeKey, stepId)
  redis.call('HINCRBY', runKey, 'active_count', -1)
end
redis.call('DEL', stepKey)
return 1
`;

export const SET_STEP_STATUS_SCRIPT = `
local stepKey = KEYS[1]
local runKey = KEYS[2]
local activeKey = KEYS[3]
local streamKey = KEYS[4]
local newStatus = ARGV[1]
local now = ARGV[2]
local stepId = ARGV[3]
local oldStatus = redis.call('HGET', stepKey, 'status')
if not oldStatus then return 0 end
local active = { pending=1, running=1, waiting=1 }
local wasActive = active[oldStatus] ~= nil
local isActive = active[newStatus] ~= nil
if wasActive and not isActive then
  redis.call('SREM', activeKey, stepId)
  redis.call('HINCRBY', runKey, 'active_count', -1)
elseif not wasActive and isActive then
  redis.call('SADD', activeKey, stepId)
  redis.call('HINCRBY', runKey, 'active_count', 1)
end
redis.call('HSET', stepKey, 'status', newStatus, 'updated_at', now)
if newStatus == 'pending' then
  redis.call('HSET', stepKey, 'lease_owner', '', 'lease_until', '0')
end
local version = redis.call('HINCRBY', stepKey, 'version', 1)
redis.call('XADD', streamKey, '*', 'type', 'step.status', 'id', stepId, 'status', newStatus, 'updated_at', now, 'version', tostring(version))
return 1
`;

export const INCREMENT_ATTEMPTS_SCRIPT = `
local stepKey = KEYS[1]
local streamKey = KEYS[2]
local now = ARGV[1]
local stepId = ARGV[2]
if redis.call('EXISTS', stepKey) == 0 then return nil end
local attempts = redis.call('HINCRBY', stepKey, 'attempts', 1)
redis.call('HSET', stepKey, 'updated_at', now)
local version = redis.call('HINCRBY', stepKey, 'version', 1)
redis.call('XADD', streamKey, '*', 'type', 'step.attempts', 'id', stepId, 'attempts', tostring(attempts), 'updated_at', now, 'version', tostring(version))
return attempts
`;

export const DECR_JOIN_SCRIPT = `
local stepKey = KEYS[1]
local now = ARGV[1]
local remaining = redis.call('HGET', stepKey, 'join_remaining')
if not remaining or remaining == '' or remaining == 'null' then return nil end
local n = tonumber(remaining)
if not n or n <= 0 then return nil end
n = n - 1
redis.call('HSET', stepKey, 'join_remaining', tostring(n), 'updated_at', now)
return n
`;

export const COMPLETE_RUN_SCRIPT = `
local runKey = KEYS[1]
local status = redis.call('HGET', runKey, 'status')
local activeCount = tonumber(redis.call('HGET', runKey, 'active_count') or '0')
if status ~= 'running' or activeCount > 0 then return 0 end
local now = ARGV[1]
local output = ARGV[2]
local version = redis.call('HINCRBY', runKey, 'version', 1)
redis.call('HSET', runKey, 'status', 'completed', 'output', output, 'updated_at', now, 'finished_at', now, 'version', tostring(version))
return version
`;

export const FAIL_RUN_SCRIPT = `
local runKey = KEYS[1]
local status = redis.call('HGET', runKey, 'status')
local activeCount = tonumber(redis.call('HGET', runKey, 'active_count') or '0')
if status ~= 'running' or activeCount > 0 then return 0 end
local now = ARGV[1]
local output = ARGV[2]
local version = redis.call('HINCRBY', runKey, 'version', 1)
redis.call('HSET', runKey, 'status', 'failed', 'output', output, 'updated_at', now, 'finished_at', now, 'version', tostring(version))
return version
`;

export const CANCEL_RUN_SCRIPT = `
local runKey = KEYS[1]
local status = redis.call('HGET', runKey, 'status')
local activeCount = tonumber(redis.call('HGET', runKey, 'active_count') or '0')
if status ~= 'running' or activeCount > 0 then return 0 end
local now = ARGV[1]
local output = ARGV[2]
local version = redis.call('HINCRBY', runKey, 'version', 1)
redis.call('HSET', runKey, 'status', 'cancelled', 'output', output, 'updated_at', now, 'finished_at', now, 'abort_requested', '0', 'version', tostring(version))
return version
`;

export const CANCEL_FROM_QUEUE_SCRIPT = `
local inflightKey = KEYS[1]
local wfListKey = KEYS[2]
local stepKey = KEYS[3]
local stepExecId = ARGV[1]
redis.call('LREM', inflightKey, 1, stepExecId)
redis.call('LREM', wfListKey, 0, stepExecId)
local status = redis.call('HGET', stepKey, 'status')
if status == 'pending' or status == 'completed' or status == 'failed' or status == 'cancelled' or status == 'waiting' then
  redis.call('HSET', stepKey, 'queued', '0')
end
return 1
`;

export const ACQUIRE_RUN_LOCK_SCRIPT = `
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2]) then return 1 end
return 0
`;

export const RELEASE_RUN_LOCK_SCRIPT = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;

export const ACQUIRE_LEADER_LOCK_SCRIPT = `
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2]) then return 1 end
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('EXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
`;
