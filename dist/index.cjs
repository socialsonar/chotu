"use strict";
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// src/index.ts
var index_exports = {};
__export(index_exports, {
  DEFAULT_LEASE_BUFFER_MS: () => DEFAULT_LEASE_BUFFER_MS,
  DEFAULT_STEP_TIMEOUT_MS: () => DEFAULT_STEP_TIMEOUT_MS,
  FAIR_ENQUEUE_SCRIPT: () => FAIR_ENQUEUE_SCRIPT,
  Step: () => Step,
  StepExecutionStatus: () => StepExecutionStatus,
  StepRegistry: () => StepRegistry,
  Workflow: () => Workflow,
  WorkflowRunStatus: () => WorkflowRunStatus,
  computeLeaseTtlMs: () => computeLeaseTtlMs,
  createChotu: () => createChotu,
  createStepError: () => createStepError,
  defineWorkflow: () => defineWorkflow,
  getChotu: () => getChotu,
  getStepName: () => getStepName,
  inflightKey: () => inflightKey,
  isChotuStepError: () => isChotuStepError,
  isNextStep: () => isNextStep,
  isParallelSpec: () => isParallelSpec,
  next: () => next,
  parallel: () => parallel,
  queueRotationKey: () => queueRotationKey,
  queueWfKey: () => queueWfKey,
  queueWorkflowsKey: () => queueWorkflowsKey,
  resetChotu: () => resetChotu,
  resolveStepTimeoutMs: () => resolveStepTimeoutMs,
  stepKey: () => stepKey,
  validateConfig: () => validateConfig,
  validateStepQueues: () => validateStepQueues
});
module.exports = __toCommonJS(index_exports);

// src/platform/sleep.ts
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// src/platform/sql.ts
var import_postgres = __toESM(require("postgres"), 1);
function loadPostgres() {
  if (typeof import_postgres.default === "function") {
    return import_postgres.default;
  }
  const mod = import_postgres.default;
  return mod.default;
}
function createSql(url, options = {}) {
  return loadPostgres()(url, {
    max: options.max ?? 10
  });
}

// src/platform/redis.ts
var import_ioredis = require("ioredis");
function createRedis(url) {
  const client = new import_ioredis.Redis(url, {
    lazyConnect: true,
    maxRetriesPerRequest: null
  });
  return {
    async connect() {
      if (client.status === "wait" || client.status === "end") {
        await client.connect();
      }
    },
    ping() {
      return client.ping();
    },
    close() {
      client.disconnect();
    },
    send(command, args) {
      return client.call(command, ...args);
    }
  };
}

// src/engine/engine.ts
var ChotuEngine = class {
  constructor(flusher, workerPool, lifecycle, recovery) {
    this.flusher = flusher;
    this.workerPool = workerPool;
    this.lifecycle = lifecycle;
    this.recovery = recovery;
  }
  workersStarted = false;
  engineStarted = false;
  setWorkersStarted(value) {
    this.workersStarted = value;
  }
  areWorkersStarted() {
    return this.workersStarted;
  }
  async start() {
    if (this.engineStarted) return;
    this.engineStarted = true;
    this.workersStarted = true;
    await this.flusher.start();
    await this.workerPool.start();
  }
  async stop() {
    if (!this.engineStarted) return;
    this.engineStarted = false;
    this.workersStarted = false;
    await this.workerPool.stop();
    await this.flusher.stop();
  }
  async recoverOnStartup() {
    return this.recovery.recoverOnStartup();
  }
  async runWorkflow(name, input) {
    return this.lifecycle.runWorkflow(name, input);
  }
  async getWorkflowRun(id) {
    return this.lifecycle.getWorkflowRun(id);
  }
  async getStepExecutions(workflowRunId) {
    return this.lifecycle.getStepExecutions(workflowRunId);
  }
  async abortWorkflow(workflowRunId, reason) {
    const started = await this.lifecycle.beginCancelWorkflow(workflowRunId, reason);
    if (!started) return false;
    this.workerPool.abortInFlightForRun(workflowRunId);
    await this.lifecycle.finalizeCancelIfReady(workflowRunId, reason);
    return true;
  }
  async recoverStaleRunningSteps() {
    return this.recovery.recoverStaleRunningSteps();
  }
  async recoverInflightSteps() {
    return this.recovery.recoverInflightSteps();
  }
  async recoverOrphanedPendingSteps() {
    return this.recovery.recoverOrphanedPendingSteps();
  }
};

// src/engine/hook-runner.ts
var ChotuHookRunner = class {
  constructor(hooks, logger) {
    this.hooks = hooks;
    this.logger = logger;
  }
  async workflowStarted(ctx) {
    await this.invoke("onWorkflowStarted", () => this.hooks?.onWorkflowStarted?.(ctx));
  }
  async workflowCompleted(ctx) {
    await this.invoke("onWorkflowCompleted", () => this.hooks?.onWorkflowCompleted?.(ctx));
  }
  async workflowError(ctx) {
    await this.invoke("onWorkflowError", () => this.hooks?.onWorkflowError?.(ctx));
  }
  async workflowCancelled(ctx) {
    await this.invoke("onWorkflowCancelled", () => this.hooks?.onWorkflowCancelled?.(ctx));
  }
  async stepStarted(ctx) {
    await this.invoke("onStepStarted", () => this.hooks?.onStepStarted?.(ctx));
  }
  async stepCompleted(ctx) {
    await this.invoke("onStepCompleted", () => this.hooks?.onStepCompleted?.(ctx));
  }
  async stepFailed(ctx) {
    await this.invoke("onStepFailed", () => this.hooks?.onStepFailed?.(ctx));
  }
  async stepCancelled(ctx) {
    await this.invoke("onStepCancelled", () => this.hooks?.onStepCancelled?.(ctx));
  }
  async invoke(name, fn) {
    if (!this.hooks) return;
    try {
      await fn();
    } catch (err) {
      this.logger.error(`[chotu] Hook ${name} failed:`, err);
    }
  }
};

// src/domain/step.ts
function isChotuStepError(value) {
  return typeof value === "object" && value !== null && "__chotuError" in value && typeof value.__chotuError?.message === "string";
}
function createStepError(message, stepName) {
  return { __chotuError: { message, stepName } };
}
function getStepName(cls) {
  const named = cls;
  return named.stepName ?? cls.name;
}
function getStepTimeoutMs(cls) {
  const timeoutMs = cls.timeoutMs;
  return timeoutMs;
}
var Step = class {
  static stepName;
  static timeoutMs;
  async onBeforeRun(_input, _ctx, _signal) {
  }
  async onAfterRun(_input, _output, _ctx, _signal) {
  }
  async onError(_input, _error, _ctx, _signal) {
  }
};
function next(step, input) {
  return { step, input };
}
function parallel(branches, join) {
  return { type: "parallel", branches, join };
}
function isParallelSpec(value) {
  return typeof value === "object" && value !== null && "type" in value && value.type === "parallel";
}
function isNextStep(value) {
  return typeof value === "object" && value !== null && "step" in value && "input" in value;
}

// src/domain/timeout.ts
var DEFAULT_STEP_TIMEOUT_MS = 6e4;
var DEFAULT_LEASE_BUFFER_MS = 3e4;
function computeLeaseTtlMs(stepTimeoutMs, leaseBufferMs) {
  return stepTimeoutMs + leaseBufferMs;
}
function resolveStepTimeoutMs(stepTimeoutMs, defaultStepTimeoutMs) {
  return stepTimeoutMs ?? defaultStepTimeoutMs;
}

// src/domain/workflow.ts
var Workflow = class {
  completeStep;
  terminalSteps;
  async onBeforeStart(_input, _ctx, _signal) {
  }
  async onAfterCompleted(_input, _output, _ctx, _signal) {
  }
};
function stepInList(step, steps) {
  const name = getStepName(step);
  return steps.some((s) => getStepName(s) === name);
}
function validateWorkflowInstance(instance2) {
  if (!instance2.name?.trim()) {
    throw new Error("[chotu] Workflow name is required");
  }
  if (!stepInList(instance2.firstStep, instance2.steps)) {
    throw new Error(
      `[chotu] Workflow "${instance2.name}": firstStep "${getStepName(instance2.firstStep)}" must be in steps`
    );
  }
  if (instance2.completeStep && !stepInList(instance2.completeStep, instance2.steps)) {
    throw new Error(
      `[chotu] Workflow "${instance2.name}": completeStep "${getStepName(instance2.completeStep)}" must be in steps`
    );
  }
  if (instance2.terminalSteps) {
    for (const terminal of instance2.terminalSteps) {
      if (!stepInList(terminal, instance2.steps)) {
        throw new Error(
          `[chotu] Workflow "${instance2.name}": terminalStep "${getStepName(terminal)}" must be in steps`
        );
      }
    }
  }
  const names = instance2.steps.map(getStepName);
  const duplicates = names.filter((n, i) => names.indexOf(n) !== i);
  if (duplicates.length > 0) {
    throw new Error(
      `[chotu] Workflow "${instance2.name}": duplicate step names: ${[...new Set(duplicates)].join(", ")}`
    );
  }
  if (!instance2.completeStep && !instance2.terminalSteps?.length) {
    throw new Error(
      `[chotu] Workflow "${instance2.name}": define completeStep or terminalSteps for workflow completion output`
    );
  }
}
function defineWorkflow(workflowOrClass) {
  const instance2 = typeof workflowOrClass === "function" ? new workflowOrClass() : workflowOrClass;
  validateWorkflowInstance(instance2);
  return instance2;
}
function validateStepQueues(stepQueues, workflows) {
  validateConfig([], stepQueues, workflows);
}
function validateConfig(queues, stepQueues, workflows) {
  if (!queues.length) {
    throw new Error("[chotu] At least one queue must be configured");
  }
  const queueNames = /* @__PURE__ */ new Set();
  for (const queue of queues) {
    if (!queue.name?.trim()) {
      throw new Error("[chotu] Queue name is required");
    }
    if (queue.concurrency < 1) {
      throw new Error(`[chotu] Queue "${queue.name}": concurrency must be >= 1`);
    }
    if (queue.maxRetries != null && queue.maxRetries < 0) {
      throw new Error(`[chotu] Queue "${queue.name}": maxRetries must be >= 0`);
    }
    queueNames.add(queue.name);
  }
  const registered = /* @__PURE__ */ new Set();
  const stepToWorkflow = /* @__PURE__ */ new Map();
  for (const workflow of workflows) {
    for (const step of workflow.steps) {
      const stepName = getStepName(step);
      registered.add(stepName);
      const existingWorkflow = stepToWorkflow.get(stepName);
      if (existingWorkflow && existingWorkflow !== workflow.name) {
        throw new Error(
          `[chotu] Duplicate step name "${stepName}" in workflows "${existingWorkflow}" and "${workflow.name}"`
        );
      }
      stepToWorkflow.set(stepName, workflow.name);
      const resolvedQueue = stepQueues[stepName] ?? "default";
      if (!queueNames.has(resolvedQueue)) {
        throw new Error(
          `[chotu] Step "${stepName}" (workflow "${workflow.name}") resolves to queue "${resolvedQueue}" which is not configured`
        );
      }
      const timeoutMs = getStepTimeoutMs(step);
      if (timeoutMs != null && timeoutMs < 1) {
        throw new Error(
          `[chotu] Step "${stepName}" (workflow "${workflow.name}"): timeoutMs must be >= 1`
        );
      }
    }
  }
  for (const stepName of Object.keys(stepQueues)) {
    if (!registered.has(stepName)) {
      throw new Error(`[chotu] stepQueues key "${stepName}" is not a registered step`);
    }
    if (!queueNames.has(stepQueues[stepName])) {
      throw new Error(
        `[chotu] stepQueues["${stepName}"] references unconfigured queue "${stepQueues[stepName]}"`
      );
    }
  }
}

// src/engine/step-registry.ts
var StepRegistry = class {
  stepClasses = /* @__PURE__ */ new Map();
  workflows = /* @__PURE__ */ new Map();
  queues = /* @__PURE__ */ new Map();
  stepQueues;
  defaultStepTimeoutMs;
  leaseBufferMs;
  constructor(queueConfigs, stepQueues, workflowDefinitions, options = {}) {
    const defaultStepTimeoutMs = options.defaultStepTimeoutMs ?? DEFAULT_STEP_TIMEOUT_MS;
    const leaseBufferMs = options.leaseBufferMs ?? DEFAULT_LEASE_BUFFER_MS;
    if (defaultStepTimeoutMs < 1) {
      throw new Error("[chotu] defaultStepTimeoutMs must be >= 1");
    }
    if (leaseBufferMs < 0) {
      throw new Error("[chotu] leaseBufferMs must be >= 0");
    }
    this.defaultStepTimeoutMs = defaultStepTimeoutMs;
    this.leaseBufferMs = leaseBufferMs;
    validateConfig(queueConfigs, stepQueues, workflowDefinitions);
    for (const queue of queueConfigs) {
      this.queues.set(queue.name, queue);
    }
    this.stepQueues = stepQueues;
    for (const workflow of workflowDefinitions) {
      this.workflows.set(workflow.name, workflow);
      for (const stepClass of workflow.steps) {
        this.stepClasses.set(getStepName(stepClass), stepClass);
      }
    }
  }
  getWorkflow(name) {
    return this.workflows.get(name);
  }
  getStepClass(stepName) {
    return this.stepClasses.get(stepName);
  }
  getQueue(name) {
    return this.queues.get(name);
  }
  queueNames() {
    return this.queues.keys();
  }
  allQueues() {
    return [...this.queues.values()];
  }
  resolveQueue(stepName) {
    const queueName = this.stepQueues[stepName] ?? "default";
    if (!this.queues.has(queueName)) {
      throw new Error(`[chotu] Queue "${queueName}" not configured (step "${stepName}")`);
    }
    return queueName;
  }
  getStepTimeoutOverrideMs(stepName) {
    const stepClass = this.stepClasses.get(stepName);
    if (!stepClass) return void 0;
    return getStepTimeoutMs(stepClass);
  }
  getEffectiveStepTimeoutMs(stepName) {
    return resolveStepTimeoutMs(this.getStepTimeoutOverrideMs(stepName), this.defaultStepTimeoutMs);
  }
  getLeaseTtlMs(stepName) {
    return computeLeaseTtlMs(this.getEffectiveStepTimeoutMs(stepName), this.leaseBufferMs);
  }
  getEffectiveMaxAttempts(queue, error) {
    const maxRetries = queue.maxRetries ?? 3;
    const baseAttempts = maxRetries + 1;
    if (this.isTransientDbError(error)) {
      return baseAttempts + 2;
    }
    return baseAttempts;
  }
  isTransientDbError(error) {
    const msg = error.message.toLowerCase();
    return msg.includes("failed to read data") || msg.includes("connection") || msg.includes("timeout") || error.code === "ERR_POSTGRES_INVALID_MESSAGE";
  }
};

// src/interfaces/workflow.interface.ts
var WorkflowRunStatus = /* @__PURE__ */ ((WorkflowRunStatus2) => {
  WorkflowRunStatus2["RUNNING"] = "running";
  WorkflowRunStatus2["COMPLETED"] = "completed";
  WorkflowRunStatus2["FAILED"] = "failed";
  WorkflowRunStatus2["CANCELLED"] = "cancelled";
  return WorkflowRunStatus2;
})(WorkflowRunStatus || {});
var StepExecutionStatus = /* @__PURE__ */ ((StepExecutionStatus2) => {
  StepExecutionStatus2["PENDING"] = "pending";
  StepExecutionStatus2["RUNNING"] = "running";
  StepExecutionStatus2["COMPLETED"] = "completed";
  StepExecutionStatus2["FAILED"] = "failed";
  StepExecutionStatus2["WAITING"] = "waiting";
  StepExecutionStatus2["CANCELLED"] = "cancelled";
  return StepExecutionStatus2;
})(StepExecutionStatus || {});

// src/engine/workflow-lifecycle.ts
var DEFAULT_BEFORE_START_TIMEOUT_MS = 3e4;
async function syncWithRetry(fn, label, logger, maxAttempts = 3) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await fn();
      return;
    } catch (err) {
      if (attempt === maxAttempts) {
        logger.error(`[chotu] ${label} failed after ${maxAttempts} attempts:`, err);
        return;
      }
      await sleep(100 * attempt);
    }
  }
}
var WorkflowLifecycle = class {
  constructor(stateStore, repository, fairQueue, registry, logger, hookRunner, runPurger) {
    this.stateStore = stateStore;
    this.repository = repository;
    this.fairQueue = fairQueue;
    this.registry = registry;
    this.logger = logger;
    this.hookRunner = hookRunner;
    this.runPurger = runPurger;
  }
  async runWorkflow(name, input) {
    const workflow = this.registry.getWorkflow(name);
    if (!workflow) {
      throw new Error(`[chotu] Workflow "${name}" not registered`);
    }
    const workflowRunId = crypto.randomUUID();
    const hookCtx = {
      workflowRunId,
      workflowName: name,
      input
    };
    let effectiveInput = input;
    const beforeStartResult = await workflow.onBeforeStart(
      input,
      hookCtx,
      AbortSignal.timeout(DEFAULT_BEFORE_START_TIMEOUT_MS)
    );
    if (beforeStartResult !== void 0) {
      effectiveInput = beforeStartResult;
    }
    const effectiveInputRecord = effectiveInput;
    const firstStepId = crypto.randomUUID();
    const firstStepName = getStepName(workflow.firstStep);
    const queue = this.registry.resolveQueue(firstStepName);
    await this.stateStore.createRun({
      id: workflowRunId,
      workflowName: name,
      input: effectiveInputRecord
    });
    const created = await this.stateStore.createStep({
      id: firstStepId,
      workflowRunId,
      stepName: firstStepName,
      queue,
      input: effectiveInputRecord
    });
    if (!created) {
      await this.stateStore.rollbackRun(workflowRunId);
      throw new Error(
        `[chotu] Failed to create first step in Redis for workflow ${workflowRunId}`
      );
    }
    try {
      await this.repository.insertWorkflowRunWithFirstStep({
        workflowRunId,
        workflowName: name,
        input: effectiveInputRecord,
        firstStepId,
        firstStepName,
        queue
      });
    } catch (err) {
      await this.stateStore.rollbackStep(firstStepId, workflowRunId, firstStepName);
      await this.stateStore.rollbackRun(workflowRunId);
      throw err;
    }
    await this.enqueueStep(firstStepId, firstStepName, workflowRunId);
    this.logger.info(`[chotu] Workflow run ${workflowRunId} ("${name}") started`);
    await this.hookRunner.workflowStarted({
      workflowRunId,
      workflowName: name,
      input: effectiveInputRecord
    });
    return { id: workflowRunId };
  }
  async getWorkflowRun(id) {
    return this.repository.getWorkflowRun(id);
  }
  async getStepExecutions(workflowRunId) {
    return this.repository.getStepExecutions(workflowRunId);
  }
  async loadStep(stepExecId) {
    return this.stateStore.loadStep(stepExecId);
  }
  async loadRun(workflowRunId) {
    return this.stateStore.loadRun(workflowRunId);
  }
  async setStepStatus(stepExecId, status) {
    const ok = await this.stateStore.setStepStatus(stepExecId, status);
    if (!ok) {
      this.logger.warn(
        `[chotu] setStepStatus(${stepExecId}, ${status}) rejected \u2014 step missing`
      );
    }
    return ok;
  }
  async incrementAttempts(stepExecId) {
    return this.stateStore.incrementAttempts(stepExecId);
  }
  async completeStep(stepExecId, output) {
    const updated = await this.stateStore.completeStep(stepExecId, output);
    if (!updated) return;
    await syncWithRetry(
      () => this.repository.syncStepTerminal({
        id: stepExecId,
        status: "completed" /* COMPLETED */,
        output,
        version: updated.version
      }),
      `syncStepTerminal(completed, ${stepExecId})`,
      this.logger
    );
  }
  async failStep(stepExecId, row, error) {
    if (await this.isAbortRequested(row.workflow_run_id)) {
      await this.cancelStep(stepExecId, row);
      return;
    }
    const updated = await this.stateStore.failStep(stepExecId, { message: error.message });
    if (!updated) return;
    await syncWithRetry(
      () => this.repository.syncStepTerminal({
        id: stepExecId,
        status: "failed" /* FAILED */,
        error: { message: error.message },
        version: updated.version
      }),
      `syncStepTerminal(failed, ${stepExecId})`,
      this.logger
    );
    if (row.join_step_id && row.fan_out_index != null) {
      await this.decrementJoinRemaining(row.join_step_id, row.workflow_run_id);
    }
    await this.checkCompletion(row.workflow_run_id);
    this.logger.info(
      `[chotu] Step ${stepExecId} failed (workflow ${row.workflow_run_id} continues)`
    );
  }
  async cancelStep(stepExecId, row, reason) {
    const updated = await this.stateStore.cancelStep(stepExecId, reason);
    if (!updated) return;
    await syncWithRetry(
      () => this.repository.syncStepTerminal({
        id: stepExecId,
        status: "cancelled" /* CANCELLED */,
        error: reason ? { reason } : null,
        version: updated.version
      }),
      `syncStepTerminal(cancelled, ${stepExecId})`,
      this.logger
    );
    const runRow = await this.stateStore.loadRun(row.workflow_run_id);
    if (runRow) {
      await this.hookRunner.stepCancelled({
        stepExecId,
        stepName: row.step_name,
        queue: row.queue,
        workflowRunId: row.workflow_run_id,
        workflowName: runRow.workflow_name,
        attempt: row.attempts + 1,
        reason
      });
    }
    if (row.join_step_id && row.fan_out_index != null) {
      await this.decrementJoinRemaining(row.join_step_id, row.workflow_run_id);
    }
    await this.finalizeCancelIfReady(row.workflow_run_id, reason);
    this.logger.info(
      `[chotu] Step ${stepExecId} cancelled (workflow ${row.workflow_run_id})`
    );
  }
  async isAbortRequested(workflowRunId) {
    return this.stateStore.isAbortRequested(workflowRunId);
  }
  async canScheduleForRun(workflowRunId) {
    const status = await this.stateStore.getRunStatus(workflowRunId);
    if (status !== "running" /* RUNNING */) return false;
    if (await this.isAbortRequested(workflowRunId)) return false;
    return true;
  }
  async beginCancelWorkflow(workflowRunId, reason) {
    const run = await this.stateStore.loadRun(workflowRunId);
    if (!run) return false;
    if (run.status !== "running" /* RUNNING */) return false;
    if (await this.isAbortRequested(workflowRunId)) return false;
    await this.stateStore.markAbortRequested(workflowRunId);
    const steps = await this.stateStore.listStepsForRun(workflowRunId);
    const cancellable = /* @__PURE__ */ new Set([
      "pending" /* PENDING */,
      "waiting" /* WAITING */
    ]);
    for (const step of steps) {
      if (!cancellable.has(step.status)) continue;
      await this.fairQueue.cancelFromQueue(step.queue, step.id, workflowRunId);
      await this.cancelStep(step.id, step, reason);
    }
    return true;
  }
  async finalizeCancelIfReady(workflowRunId, reason) {
    if (!await this.isAbortRequested(workflowRunId)) return false;
    const activeCount = await this.stateStore.getActiveCount(workflowRunId);
    if (activeCount > 0) return false;
    await this.finalizeCancelledRun(workflowRunId, reason);
    return true;
  }
  async finalizeCancelledRun(workflowRunId, reason) {
    const output = reason ? { reason } : null;
    const version = await this.stateStore.tryCancelRun(workflowRunId, reason);
    if (version == null) return;
    await syncWithRetry(
      async () => {
        const synced = await this.repository.syncWorkflowTerminal({
          id: workflowRunId,
          status: "cancelled" /* CANCELLED */,
          output,
          version
        });
        if (!synced) {
          throw new Error(
            `syncWorkflowTerminal(cancelled) returned false for ${workflowRunId}`
          );
        }
      },
      `syncWorkflowTerminal(cancelled, ${workflowRunId})`,
      this.logger
    );
    this.logger.info(
      `[chotu] Workflow run ${workflowRunId} cancelled${reason ? `: ${reason}` : ""}`
    );
    const runRow = await this.stateStore.loadRun(workflowRunId);
    if (runRow) {
      await this.hookRunner.workflowCancelled({
        workflowRunId,
        workflowName: runRow.workflow_name,
        input: runRow.input,
        reason
      });
    }
    await this.runPurger.purgeTerminalRun(workflowRunId);
  }
  async scheduleNext(nextSteps, workflowRunId, currentStepExecId, stepOutput) {
    if (!await this.canScheduleForRun(workflowRunId)) return;
    const currentRow = await this.loadStep(currentStepExecId);
    if (!currentRow) return;
    if (nextSteps === "END") {
      if (currentRow.join_step_id) {
        await this.completeBranch(currentRow);
      }
      return;
    }
    if (isNextStep(nextSteps)) {
      const stepName = getStepName(nextSteps.step);
      const nextStepId = await this.createStepExecution({
        workflowRunId,
        stepName,
        queue: this.registry.resolveQueue(stepName),
        input: nextSteps.input,
        joinStepId: currentRow.join_step_id,
        fanOutIndex: currentRow.fan_out_index
      });
      await this.enqueueStep(nextStepId, stepName, workflowRunId);
      return;
    }
    if (isParallelSpec(nextSteps)) {
      await this.spawnParallel(nextSteps, workflowRunId);
    }
  }
  async checkCompletion(workflowRunId) {
    const lockToken = crypto.randomUUID();
    const acquired = await this.stateStore.acquireRunLock(workflowRunId, lockToken, 30);
    if (!acquired) return;
    try {
      await this.doCheckCompletion(workflowRunId);
    } finally {
      await this.stateStore.releaseRunLock(workflowRunId, lockToken);
    }
  }
  async enqueueStep(stepExecId, stepName, workflowRunId) {
    if (!await this.canScheduleForRun(workflowRunId)) return;
    const queueName = this.registry.resolveQueue(stepName);
    await this.fairQueue.enqueueWithRetry(stepExecId, queueName, workflowRunId);
  }
  async decrementJoinRemaining(joinStepId, workflowRunId) {
    if (!await this.canScheduleForRun(workflowRunId)) {
      await this.finalizeCancelIfReady(workflowRunId);
      return;
    }
    const remaining = await this.stateStore.decrementJoinRemaining(joinStepId);
    if (remaining == null) return;
    await this.repository.syncJoinRemaining(joinStepId, remaining);
    if (remaining > 0) {
      await this.checkCompletion(workflowRunId);
      return;
    }
    await this.finalizeJoin(joinStepId, workflowRunId);
  }
  async finalizeJoin(joinStepId, workflowRunId) {
    if (!await this.canScheduleForRun(workflowRunId)) {
      await this.finalizeCancelIfReady(workflowRunId);
      return;
    }
    const joinRow = await this.loadStep(joinStepId);
    if (!joinRow) {
      this.logger.error(
        `[chotu] Join step ${joinStepId} missing when finalizing workflow ${workflowRunId}`
      );
      await this.failWorkflowRun(workflowRunId, "Join step missing during finalize");
      return;
    }
    const branches = await this.stateStore.getJoinBranches(joinStepId);
    const outputs = branches.map((branch) => {
      if (branch.status === "failed" /* FAILED */) {
        const branchError = branch.error;
        return createStepError(
          branchError?.message ?? "Branch failed",
          branch.step_name ?? "unknown"
        );
      }
      const output = branch.output;
      if (output && isChotuStepError(output)) {
        return output;
      }
      return output ?? {};
    });
    const updated = await this.stateStore.finalizeJoinStep(joinStepId, outputs);
    if (!updated) return;
    await syncWithRetry(
      () => this.repository.syncJoinFinalize({
        id: joinStepId,
        input: outputs,
        version: updated.version
      }),
      `syncJoinFinalize(${joinStepId})`,
      this.logger
    );
    await this.enqueueStep(joinStepId, joinRow.step_name, workflowRunId);
  }
  async failWorkflowRun(workflowRunId, reason) {
    const output = reason ? { reason } : null;
    const version = await this.stateStore.tryFailRun(workflowRunId, reason);
    if (version == null) return;
    await syncWithRetry(
      async () => {
        const synced = await this.repository.syncWorkflowTerminal({
          id: workflowRunId,
          status: "failed" /* FAILED */,
          output,
          version
        });
        if (!synced) {
          throw new Error(`syncWorkflowTerminal(failed) returned false for ${workflowRunId}`);
        }
      },
      `syncWorkflowTerminal(failed, ${workflowRunId})`,
      this.logger
    );
    this.logger.info(
      `[chotu] Workflow run ${workflowRunId} failed${reason ? `: ${reason}` : ""}`
    );
    const runRow = await this.stateStore.loadRun(workflowRunId);
    if (runRow) {
      await this.hookRunner.workflowError({
        workflowRunId,
        workflowName: runRow.workflow_name,
        input: runRow.input,
        reason
      });
    }
    await this.runPurger.purgeTerminalRun(workflowRunId);
  }
  async createStepExecution(params) {
    if (!await this.canScheduleForRun(params.workflowRunId)) {
      throw new Error(
        `[chotu] Cannot create step for non-running workflow ${params.workflowRunId}`
      );
    }
    const id = crypto.randomUUID();
    const status = params.status ?? "pending" /* PENDING */;
    const created = await this.stateStore.createStep({
      id,
      workflowRunId: params.workflowRunId,
      stepName: params.stepName,
      queue: params.queue,
      status,
      input: params.input,
      joinStepId: params.joinStepId,
      fanOutIndex: params.fanOutIndex,
      joinTotal: params.joinTotal,
      joinRemaining: params.joinRemaining
    });
    if (!created) {
      throw new Error(
        `[chotu] Active step already exists for "${params.stepName}" in run ${params.workflowRunId}`
      );
    }
    try {
      await this.repository.insertStep({
        id,
        workflowRunId: params.workflowRunId,
        stepName: params.stepName,
        queue: params.queue,
        status,
        input: params.input,
        joinStepId: params.joinStepId,
        fanOutIndex: params.fanOutIndex,
        joinTotal: params.joinTotal,
        joinRemaining: params.joinRemaining
      });
    } catch (err) {
      await this.stateStore.rollbackStep(id, params.workflowRunId, params.stepName);
      throw err;
    }
    return id;
  }
  async completeBranch(row) {
    if (!row.join_step_id || row.fan_out_index == null) return;
    await this.decrementJoinRemaining(row.join_step_id, row.workflow_run_id);
  }
  async spawnParallel(spec, workflowRunId) {
    let joinStepId = null;
    if (spec.join) {
      const joinStepName = getStepName(spec.join);
      joinStepId = await this.createStepExecution({
        workflowRunId,
        stepName: joinStepName,
        queue: this.registry.resolveQueue(joinStepName),
        input: null,
        status: "waiting" /* WAITING */,
        joinTotal: spec.branches.length,
        joinRemaining: spec.branches.length
      });
    }
    for (let i = 0; i < spec.branches.length; i++) {
      const branch = spec.branches[i];
      const stepName = getStepName(branch.step);
      const branchStepId = await this.createStepExecution({
        workflowRunId,
        stepName,
        queue: this.registry.resolveQueue(stepName),
        input: branch.input,
        joinStepId,
        fanOutIndex: i
      });
      await this.enqueueStep(branchStepId, stepName, workflowRunId);
    }
  }
  async doCheckCompletion(workflowRunId) {
    const runStatus = await this.stateStore.getRunStatus(workflowRunId);
    if (runStatus === "completed" /* COMPLETED */ || runStatus === "failed" /* FAILED */ || runStatus === "cancelled" /* CANCELLED */) {
      return;
    }
    if (await this.isAbortRequested(workflowRunId)) {
      await this.finalizeCancelIfReady(workflowRunId);
      return;
    }
    const activeCount = await this.stateStore.getActiveCount(workflowRunId);
    if (activeCount > 0) return;
    const unabsorbedFailures = await this.stateStore.countUnabsorbedFailures(workflowRunId);
    if (unabsorbedFailures > 0) {
      await this.failWorkflowRun(workflowRunId);
      return;
    }
    const runRow = await this.stateStore.loadRun(workflowRunId);
    if (!runRow) return;
    const workflow = this.registry.getWorkflow(runRow.workflow_name);
    if (workflow?.completeStep) {
      await this.handleCompleteStep(workflowRunId, workflow, runRow);
      return;
    }
    await this.completeWorkflowWithoutCompleteStep(workflowRunId, workflow);
  }
  async handleCompleteStep(workflowRunId, workflow, runRow) {
    const completeStepName = getStepName(workflow.completeStep);
    let completeStepId = null;
    const completeRow = await this.stateStore.findStepByName(workflowRunId, completeStepName);
    if (!completeRow) {
      const queue = this.registry.resolveQueue(completeStepName);
      const input = {
        workflowInput: runRow.input,
        workflowRunId
      };
      completeStepId = await this.createStepExecution({
        workflowRunId,
        stepName: completeStepName,
        queue,
        input
      });
    } else if (completeRow.status === "completed" /* COMPLETED */) {
      const version = await this.stateStore.tryCompleteRun(
        workflowRunId,
        completeRow.output ?? null
      );
      if (version == null) return;
      await syncWithRetry(
        async () => {
          const synced = await this.repository.completeWorkflowFromCompleteStep({
            workflowRunId,
            output: completeRow.output ?? null,
            version
          });
          if (!synced) {
            throw new Error(
              `completeWorkflowFromCompleteStep returned false for ${workflowRunId}`
            );
          }
        },
        `completeWorkflowFromCompleteStep(${workflowRunId})`,
        this.logger
      );
      this.logger.info(`[chotu] Workflow run ${workflowRunId} completed`);
      const output = completeRow.output ?? null;
      await this.invokeWorkflowHook(
        "onAfterCompleted",
        () => workflow.onAfterCompleted(
          runRow.input,
          output,
          {
            workflowRunId,
            workflowName: workflow.name,
            input: runRow.input
          },
          AbortSignal.timeout(DEFAULT_BEFORE_START_TIMEOUT_MS)
        )
      );
      await this.hookRunner.workflowCompleted({
        workflowRunId,
        workflowName: workflow.name,
        input: runRow.input,
        output
      });
      await this.runPurger.purgeTerminalRun(workflowRunId);
      return;
    } else if (completeRow.status === "pending" /* PENDING */ || completeRow.status === "running" /* RUNNING */) {
      completeStepId = completeRow.id;
    }
    if (completeStepId) {
      await this.enqueueStep(completeStepId, completeStepName, workflowRunId);
    }
  }
  async completeWorkflowWithoutCompleteStep(workflowRunId, workflow) {
    let output = null;
    if (workflow?.terminalSteps?.length) {
      const terminalNames = workflow.terminalSteps.map(getStepName);
      const terminalRows = [];
      for (const name of terminalNames) {
        const row = await this.stateStore.findStepByName(workflowRunId, name);
        if (row?.status === "completed" /* COMPLETED */) {
          terminalRows.push({ step_name: name, output: row.output ?? null });
        }
      }
      if (terminalRows.length === 1) {
        output = terminalRows[0]?.output ?? null;
      } else if (terminalRows.length > 1) {
        output = Object.fromEntries(
          terminalRows.map((r) => [r.step_name, r.output ?? null])
        );
      }
    }
    const version = await this.stateStore.tryCompleteRun(workflowRunId, output);
    if (version == null) return;
    await syncWithRetry(
      async () => {
        const synced = await this.repository.syncWorkflowTerminal({
          id: workflowRunId,
          status: "completed" /* COMPLETED */,
          output,
          version
        });
        if (!synced) {
          throw new Error(`syncWorkflowTerminal(completed) returned false for ${workflowRunId}`);
        }
      },
      `syncWorkflowTerminal(completed, ${workflowRunId})`,
      this.logger
    );
    this.logger.info(`[chotu] Workflow run ${workflowRunId} completed`);
    const runRow = await this.stateStore.loadRun(workflowRunId);
    if (runRow) {
      if (workflow) {
        await this.invokeWorkflowHook(
          "onAfterCompleted",
          () => workflow.onAfterCompleted(
            runRow.input,
            output,
            {
              workflowRunId,
              workflowName: runRow.workflow_name,
              input: runRow.input
            },
            AbortSignal.timeout(DEFAULT_BEFORE_START_TIMEOUT_MS)
          )
        );
      }
      await this.hookRunner.workflowCompleted({
        workflowRunId,
        workflowName: runRow.workflow_name,
        input: runRow.input,
        output
      });
    }
    await this.runPurger.purgeTerminalRun(workflowRunId);
  }
  async invokeWorkflowHook(name, fn) {
    try {
      await fn();
    } catch (err) {
      this.logger.error(`[chotu] Workflow hook ${name} failed:`, err);
    }
  }
};

// src/engine/step-executor.ts
var StepExecutor = class {
  constructor(lifecycle, registry, fairQueue, logger, hookRunner, resolveStep) {
    this.lifecycle = lifecycle;
    this.registry = registry;
    this.fairQueue = fairQueue;
    this.logger = logger;
    this.hookRunner = hookRunner;
    this.resolveStep = resolveStep;
  }
  async processStepExecution(row, queue, signal) {
    const stepExecId = row.id;
    const stepClass = this.registry.getStepClass(row.step_name);
    if (!stepClass) {
      this.logger.error(`[chotu] Step class "${row.step_name}" not registered`);
      await this.lifecycle.failStep(
        stepExecId,
        row,
        new Error(`Step "${row.step_name}" not registered`)
      );
      return false;
    }
    const runRow = await this.lifecycle.loadRun(row.workflow_run_id);
    const workflowName = runRow?.workflow_name ?? "unknown";
    const step = this.resolveStep?.(stepClass) ?? new stepClass();
    const input = this.normalizeInput(row.input);
    const attempts = row.attempts;
    const ctx = {
      stepExecId,
      stepName: row.step_name,
      queue: row.queue,
      workflowRunId: row.workflow_run_id,
      workflowName,
      attempt: attempts + 1
    };
    const shutdownSignal = signal;
    const timeoutMs = this.registry.getEffectiveStepTimeoutMs(row.step_name);
    const { stepSignal, cleanup } = this.createStepSignal(
      shutdownSignal,
      timeoutMs,
      row.step_name
    );
    try {
      const execute = async () => {
        await this.throwIfRunAborted(row.workflow_run_id);
        this.throwIfAborted(shutdownSignal);
        await step.onBeforeRun(input, ctx, stepSignal);
        await this.throwIfRunAborted(row.workflow_run_id);
        this.throwIfAborted(shutdownSignal);
        const output = await step.run(input, stepSignal);
        await this.throwIfRunAborted(row.workflow_run_id);
        this.throwIfAborted(shutdownSignal);
        await step.onAfterRun(input, output, ctx, stepSignal);
        const nextSteps = await step.getNextSteps(input, output, stepSignal);
        if (!await this.lifecycle.canScheduleForRun(row.workflow_run_id)) {
          return;
        }
        await this.lifecycle.completeStep(stepExecId, output);
        if (nextSteps === "END") {
          if (row.join_step_id) {
            await this.lifecycle.scheduleNext(
              nextSteps,
              row.workflow_run_id,
              stepExecId,
              output
            );
          }
          await this.hookRunner.stepCompleted({ ...ctx, output });
          await this.lifecycle.checkCompletion(row.workflow_run_id);
        } else {
          await this.lifecycle.scheduleNext(
            nextSteps,
            row.workflow_run_id,
            stepExecId,
            output
          );
          await this.hookRunner.stepCompleted({ ...ctx, output });
        }
      };
      await this.raceWithStepTimeout(
        execute,
        stepSignal,
        shutdownSignal,
        timeoutMs,
        row.step_name
      );
      return false;
    } catch (err) {
      if (shutdownSignal.aborted) {
        if (await this.lifecycle.isAbortRequested(row.workflow_run_id)) {
          await this.lifecycle.cancelStep(stepExecId, row);
          return false;
        }
        if (await this.lifecycle.setStepStatus(stepExecId, "pending" /* PENDING */)) {
          await this.fairQueue.requeue(row.queue, stepExecId, row.workflow_run_id);
        }
        return true;
      }
      if (await this.lifecycle.isAbortRequested(row.workflow_run_id)) {
        await this.lifecycle.cancelStep(stepExecId, row);
        return false;
      }
      const error = err instanceof Error ? err : new Error(String(err));
      this.logger.error(
        `[chotu] Step ${stepExecId} ("${row.step_name}") failed (attempt ${attempts + 1}):`,
        error
      );
      await step.onError(input, error, ctx, stepSignal).catch((onErrorErr) => {
        this.logger.error(
          `[chotu] Step ${stepExecId} ("${row.step_name}") onError handler failed:`,
          onErrorErr
        );
      });
      const effectiveMax = this.registry.getEffectiveMaxAttempts(queue, error);
      if (attempts + 1 < effectiveMax) {
        await this.hookRunner.stepFailed({ ...ctx, error, willRetry: true });
        await this.lifecycle.incrementAttempts(stepExecId);
        if (await this.lifecycle.setStepStatus(stepExecId, "pending" /* PENDING */)) {
          await this.fairQueue.requeue(row.queue, stepExecId, row.workflow_run_id);
        }
        return true;
      }
      await this.hookRunner.stepFailed({ ...ctx, error, willRetry: false });
      await this.lifecycle.failStep(stepExecId, row, error);
      return false;
    } finally {
      cleanup();
    }
  }
  async recoverFromWorkerError(stepExecId, row, queue) {
    const current = await this.lifecycle.loadStep(stepExecId);
    if (!current) return;
    if (current.status === "running" /* RUNNING */) {
      await this.lifecycle.setStepStatus(stepExecId, "pending" /* PENDING */);
    }
    await this.fairQueue.requeue(queue.name, stepExecId, row.workflow_run_id);
  }
  normalizeInput(input) {
    if (input == null) return {};
    if (typeof input === "string") {
      try {
        return JSON.parse(input);
      } catch {
        return input;
      }
    }
    return input;
  }
  throwIfAborted(signal) {
    if (signal.aborted) {
      const err = new Error("Aborted");
      err.name = "AbortError";
      throw err;
    }
  }
  async throwIfRunAborted(workflowRunId) {
    if (await this.lifecycle.isAbortRequested(workflowRunId)) {
      const err = new Error("Aborted");
      err.name = "AbortError";
      throw err;
    }
  }
  createStepSignal(shutdownSignal, timeoutMs, stepName) {
    const timeoutController = new AbortController();
    const stepSignal = AbortSignal.any([shutdownSignal, timeoutController.signal]);
    const timer = setTimeout(() => {
      timeoutController.abort(
        new Error(`Step "${stepName}" timed out after ${timeoutMs}ms`)
      );
    }, timeoutMs);
    return {
      stepSignal,
      cleanup: () => clearTimeout(timer)
    };
  }
  raceWithStepTimeout(fn, stepSignal, shutdownSignal, timeoutMs, stepName) {
    if (stepSignal.aborted) {
      this.throwIfAborted(shutdownSignal);
      throw new Error(`Step "${stepName}" timed out after ${timeoutMs}ms`);
    }
    const fnPromise = fn();
    return Promise.race([
      fnPromise,
      new Promise((_, reject) => {
        stepSignal.addEventListener(
          "abort",
          () => {
            if (shutdownSignal.aborted) {
              const err = new Error("Aborted");
              err.name = "AbortError";
              reject(err);
              return;
            }
            const reason = stepSignal.reason;
            reject(
              reason instanceof Error ? reason : new Error(`Step "${stepName}" timed out after ${timeoutMs}ms`)
            );
          },
          { once: true }
        );
      })
    ]).finally(() => {
      fnPromise.catch(() => {
      });
    });
  }
  loadStep(stepExecId) {
    return this.lifecycle.loadStep(stepExecId);
  }
  setStepStatus(stepExecId, status) {
    return this.lifecycle.setStepStatus(stepExecId, status);
  }
  async buildStepHookContext(row) {
    const runRow = await this.lifecycle.loadRun(row.workflow_run_id);
    return {
      stepExecId: row.id,
      stepName: row.step_name,
      queue: row.queue,
      workflowRunId: row.workflow_run_id,
      workflowName: runRow?.workflow_name ?? "unknown",
      attempt: row.attempts + 1
    };
  }
  async handleAbortedStep(row) {
    if (!await this.lifecycle.isAbortRequested(row.workflow_run_id)) return;
    const active = /* @__PURE__ */ new Set([
      "pending" /* PENDING */,
      "running" /* RUNNING */,
      "waiting" /* WAITING */
    ]);
    if (!active.has(row.status)) return;
    await this.fairQueue.cancelFromQueue(row.queue, row.id, row.workflow_run_id);
    await this.lifecycle.cancelStep(row.id, row);
  }
};

// src/persistence/redis/keys.ts
var SYNC_STREAM = "chotu:sync:stream";
var SYNC_CONSUMER_GROUP = "chotu-flusher";
var RECOVERY_LEADER_KEY = "chotu:recovery:leader";
var STARTUP_RECONCILE_KEY = "chotu:startup:reconcile";
var RECOVERY_LEADER_TTL_SEC = 20;
var RECOVERY_INTERVAL_MS = 3e4;
var STARTUP_RECONCILE_TTL_SEC = 300;
function stepKey(stepExecId) {
  return `chotu:step:${stepExecId}`;
}
function runKey(workflowRunId) {
  return `chotu:run:${workflowRunId}`;
}
function runStepsKey(workflowRunId) {
  return `chotu:run:${workflowRunId}:steps`;
}
function activeStepKey(workflowRunId, stepName) {
  return `chotu:run:${workflowRunId}:active:${stepName}`;
}
function joinBranchesKey(joinStepId) {
  return `chotu:run:branches:${joinStepId}`;
}
function runLockKey(workflowRunId) {
  return `chotu:sync:lock:${workflowRunId}`;
}
function inflightKey(queueName) {
  return `chotu:queue:${queueName}:inflight`;
}
function queueWfKey(queueName, workflowRunId) {
  return `chotu:queue:${queueName}:wf:${workflowRunId}`;
}
function queueWorkflowsKey(queueName) {
  return `chotu:queue:${queueName}:workflows`;
}
function queueRotationKey(queueName) {
  return `chotu:queue:${queueName}:rotation`;
}
function rateLimitKey(queueName, windowKey) {
  return `chotu:ratelimit:${queueName}:${windowKey}`;
}

// src/engine/run-purger.ts
var RunPurger = class {
  constructor(stateStore, repository, fairQueue, registry, logger, enabled) {
    this.stateStore = stateStore;
    this.repository = repository;
    this.fairQueue = fairQueue;
    this.registry = registry;
    this.logger = logger;
    this.enabled = enabled;
  }
  async purgeTerminalRun(workflowRunId) {
    if (!this.enabled) return;
    try {
      const steps = await this.stateStore.listStepsForRun(workflowRunId);
      const stepExecIds = steps.map((step) => step.id);
      const joinBranchKeys = steps.filter((step) => step.join_total != null).map((step) => joinBranchesKey(step.id));
      await this.repository.deleteStepsForRun(workflowRunId);
      await this.fairQueue.purgeRunFromQueues(
        workflowRunId,
        this.registry.queueNames(),
        stepExecIds
      );
      await this.stateStore.purgeRun(workflowRunId, stepExecIds, joinBranchKeys);
      this.logger.info(`[chotu] Purged terminal run ${workflowRunId}`);
    } catch (err) {
      this.logger.error(`[chotu] Failed to purge terminal run ${workflowRunId}:`, err);
    }
  }
};

// src/engine/recovery.service.ts
var RecoveryService = class {
  constructor(stateStore, repository, fairQueue, lifecycle, registry, logger, redis, instanceId) {
    this.stateStore = stateStore;
    this.repository = repository;
    this.fairQueue = fairQueue;
    this.lifecycle = lifecycle;
    this.registry = registry;
    this.logger = logger;
    this.redis = redis;
    this.instanceId = instanceId;
  }
  async recoverOnStartup() {
    this.logger.info("[chotu] Multi-instance startup (non-destructive)");
    const isLeader = await this.stateStore.tryAcquireStartupReconcile(this.instanceId);
    try {
      return await this.coldStartupReconcile(isLeader);
    } finally {
      if (isLeader) {
        await this.redis.send("DEL", [STARTUP_RECONCILE_KEY]);
      }
    }
  }
  async coldStartupReconcile(isLeader) {
    let hydrated = 0;
    let enqueued = 0;
    const affectedRunIds = /* @__PURE__ */ new Set();
    const pendingRows = await this.repository.listPendingSteps();
    for (let i = 0; i < pendingRows.length; i++) {
      const row = pendingRows[i];
      affectedRunIds.add(row.workflow_run_id);
      if (i > 0 && i % 50 === 0 && isLeader) {
        await this.stateStore.tryAcquireStartupReconcile(this.instanceId);
      }
      if (!await this.stateStore.existsStep(row.id)) {
        const stepRow = await this.repository.getStepRow(row.id);
        if (stepRow) {
          if (!await this.stateStore.existsRun(row.workflow_run_id)) {
            const runRow = await this.repository.getRunForHydrate(row.workflow_run_id);
            if (runRow) {
              await this.stateStore.hydrateRunIfMissing(runRow);
              hydrated++;
            }
          }
          await this.stateStore.hydrateStepIfMissing(stepRow);
          hydrated++;
        }
      }
      if (isLeader && await this.reEnqueueIfPending(row.id, row.queue ?? "default", row.workflow_run_id)) {
        enqueued++;
      }
    }
    for (const runId of affectedRunIds) {
      await this.stateStore.recomputeRunActiveCount(runId);
    }
    if (hydrated > 0 || enqueued > 0) {
      this.logger.info(
        `[chotu] Cold reconcile hydrated=${hydrated} re-enqueued=${enqueued}`
      );
    }
    return enqueued;
  }
  async recoverStaleRunningSteps() {
    let recovered = 0;
    const stepIds = await this.stateStore.scanStepIds("chotu:step:*");
    for (const stepExecId of stepIds) {
      const row = await this.lifecycle.loadStep(stepExecId);
      if (!row || row.status !== "running" /* RUNNING */) continue;
      if (await this.shouldSkipRun(row.workflow_run_id)) continue;
      if (row.lease_until > Date.now()) continue;
      const reset = await this.stateStore.resetExpiredLease(stepExecId);
      if (!reset) continue;
      await this.fairQueue.enqueueWithRetry(
        stepExecId,
        row.queue ?? "default",
        row.workflow_run_id
      );
      recovered++;
    }
    if (recovered > 0) {
      this.logger.info(`[chotu] Recovered ${recovered} stale running step(s)`);
    }
    return recovered;
  }
  async recoverInflightSteps() {
    let recovered = 0;
    for (const queueName of this.registry.queueNames()) {
      const key = inflightKey(queueName);
      const items = await this.redis.send("LRANGE", [key, "0", "-1"]);
      if (!items?.length) continue;
      for (const stepExecId of items) {
        const row = await this.lifecycle.loadStep(stepExecId);
        if (!row) {
          await this.fairQueue.ack(queueName, stepExecId);
          continue;
        }
        if (await this.shouldSkipRun(row.workflow_run_id)) {
          await this.fairQueue.ack(queueName, stepExecId);
          continue;
        }
        if (row.status === "pending" /* PENDING */) {
          await this.fairQueue.requeue(queueName, stepExecId, row.workflow_run_id);
          recovered++;
          continue;
        }
        if (row.status === "running" /* RUNNING */ && row.lease_until <= Date.now()) {
          await this.stateStore.resetExpiredLease(stepExecId);
          await this.fairQueue.requeue(queueName, stepExecId, row.workflow_run_id);
          recovered++;
          continue;
        }
        if (row.status === "completed" /* COMPLETED */ || row.status === "failed" /* FAILED */ || row.status === "cancelled" /* CANCELLED */ || row.status === "waiting" /* WAITING */ || row.status === "running" /* RUNNING */) {
          await this.fairQueue.ack(queueName, stepExecId);
        }
      }
    }
    if (recovered > 0) {
      this.logger.info(`[chotu] Recovered ${recovered} inflight step(s)`);
    }
    return recovered;
  }
  async recoverOrphanedPendingSteps() {
    let recovered = 0;
    const stepIds = await this.stateStore.scanStepIds("chotu:step:*");
    for (const stepExecId of stepIds) {
      const row = await this.lifecycle.loadStep(stepExecId);
      if (!row || row.status !== "pending" /* PENDING */) continue;
      if (await this.shouldSkipRun(row.workflow_run_id)) continue;
      if (row.queued) continue;
      if (await this.fairQueue.isStepInAnyInflight(stepExecId, this.registry.queueNames())) {
        continue;
      }
      await this.fairQueue.enqueueWithRetry(
        stepExecId,
        row.queue ?? "default",
        row.workflow_run_id
      );
      recovered++;
    }
    if (recovered > 0) {
      this.logger.info(`[chotu] Recovered ${recovered} orphaned pending step(s)`);
    }
    return recovered;
  }
  async rebuildJoinStateFromRedis() {
    const stepIds = await this.stateStore.scanStepIds("chotu:step:*");
    const affectedRunIds = /* @__PURE__ */ new Set();
    for (const stepExecId of stepIds) {
      const row = await this.lifecycle.loadStep(stepExecId);
      if (!row || row.status !== "waiting" /* WAITING */) continue;
      if (row.join_remaining != null) continue;
      const remaining = await this.stateStore.rebuildJoinRemainingFromBranches(stepExecId);
      if (remaining == null) continue;
      await this.repository.syncJoinRemaining(stepExecId, remaining);
      affectedRunIds.add(row.workflow_run_id);
      if (remaining === 0) {
        await this.lifecycle.finalizeJoin(stepExecId, row.workflow_run_id);
      }
    }
    for (const runId of affectedRunIds) {
      await this.stateStore.recomputeRunActiveCount(runId);
    }
  }
  async reEnqueueIfPending(stepExecId, queueName, workflowRunId) {
    if (await this.shouldSkipRun(workflowRunId)) return false;
    const step = await this.lifecycle.loadStep(stepExecId);
    if (step?.status === "pending" /* PENDING */ && !step.queued && !await this.fairQueue.isStepInAnyInflight(stepExecId, this.registry.queueNames())) {
      await this.fairQueue.enqueueWithRetry(stepExecId, queueName, workflowRunId);
      return true;
    }
    return false;
  }
  async shouldSkipRun(workflowRunId) {
    if (await this.stateStore.isAbortRequested(workflowRunId)) return true;
    const status = await this.stateStore.getRunStatus(workflowRunId);
    return status !== "running" /* RUNNING */;
  }
};

// src/engine/queue-worker.ts
var QueueWorkerPool = class {
  constructor(fairQueue, stateStore, stepExecutor, recovery, registry, logger, instanceId, hookRunner) {
    this.fairQueue = fairQueue;
    this.stateStore = stateStore;
    this.stepExecutor = stepExecutor;
    this.recovery = recovery;
    this.registry = registry;
    this.logger = logger;
    this.instanceId = instanceId;
    this.hookRunner = hookRunner;
  }
  started = false;
  workers = [];
  inFlight = /* @__PURE__ */ new Set();
  inFlightStepIds = /* @__PURE__ */ new Set();
  inFlightStepNames = /* @__PURE__ */ new Map();
  lastRecoveryAt = 0;
  abortControllers = /* @__PURE__ */ new Map();
  inFlightRunIds = /* @__PURE__ */ new Map();
  isStarted() {
    return this.started;
  }
  async start() {
    if (this.started) return;
    this.started = true;
    this.lastRecoveryAt = Date.now();
    for (const queue of this.registry.allQueues()) {
      for (let i = 0; i < queue.concurrency; i++) {
        this.workers.push(this.runWorker(queue));
      }
      this.logger.info(
        `[chotu] Queue "${queue.name}" started (concurrency=${queue.concurrency})`
      );
    }
  }
  async stop() {
    if (!this.started) return;
    this.started = false;
    for (const controller of this.abortControllers.values()) {
      controller.abort();
    }
    await Promise.allSettled([...this.inFlight]);
    await Promise.allSettled(this.workers);
    this.workers = [];
    this.inFlight.clear();
    this.inFlightStepNames.clear();
    this.abortControllers.clear();
    this.inFlightRunIds.clear();
  }
  abortInFlightForRun(workflowRunId) {
    for (const [stepExecId, runId] of this.inFlightRunIds) {
      if (runId !== workflowRunId) continue;
      const controller = this.abortControllers.get(stepExecId);
      controller?.abort(new Error("workflow abort"));
    }
  }
  async runWorker(queue) {
    const pollMs = queue.pollIntervalMs ?? 500;
    while (this.started) {
      try {
        if (Date.now() - this.lastRecoveryAt > RECOVERY_INTERVAL_MS) {
          this.lastRecoveryAt = Date.now();
          if (await this.stateStore.tryAcquireRecoveryLeader(this.instanceId)) {
            await this.runLeaderRecovery();
          }
        }
        await this.renewInFlightLeases();
        const stepExecId = await this.fairQueue.pop(queue.name);
        if (!stepExecId) {
          await sleep(pollMs);
          continue;
        }
        const row = await this.stepExecutor.loadStep(stepExecId);
        if (!row) {
          this.logger.warn(`[chotu] Step execution ${stepExecId} not found after pop`);
          await this.fairQueue.ack(queue.name, stepExecId);
          continue;
        }
        if (row.status === "cancelled" /* CANCELLED */ || row.status === "completed" /* COMPLETED */ || row.status === "failed" /* FAILED */) {
          await this.fairQueue.ack(queue.name, stepExecId);
          continue;
        }
        if (await this.stateStore.isAbortRequested(row.workflow_run_id)) {
          await this.stepExecutor.handleAbortedStep(row);
          await this.fairQueue.ack(queue.name, stepExecId);
          continue;
        }
        const claimed = await this.stateStore.claimStep(
          stepExecId,
          this.instanceId,
          this.registry.getLeaseTtlMs(row.step_name)
        );
        if (!claimed) {
          await this.handleFailedClaim(stepExecId, queue.name);
          continue;
        }
        let inflightHandled = false;
        try {
          if (!await this.fairQueue.acquireRateLimit(queue)) {
            if (!await this.stepExecutor.setStepStatus(
              stepExecId,
              "pending" /* PENDING */
            )) {
              await this.fairQueue.ack(queue.name, stepExecId);
              inflightHandled = true;
              continue;
            }
            await this.fairQueue.requeue(
              queue.name,
              stepExecId,
              claimed.workflow_run_id
            );
            inflightHandled = true;
            await sleep(this.fairQueue.rateLimitBackoffMs(queue));
            continue;
          }
          const stepCtx = await this.stepExecutor.buildStepHookContext(claimed);
          await this.hookRunner.stepStarted(stepCtx);
          const controller = new AbortController();
          this.abortControllers.set(stepExecId, controller);
          this.inFlightRunIds.set(stepExecId, claimed.workflow_run_id);
          this.inFlightStepIds.add(stepExecId);
          this.inFlightStepNames.set(stepExecId, claimed.step_name);
          const work = this.stepExecutor.processStepExecution(
            claimed,
            queue,
            controller.signal
          );
          this.inFlight.add(work);
          try {
            try {
              inflightHandled = await work;
            } catch (err) {
              this.logger.error(
                `[chotu] Unexpected error processing step ${stepExecId} ("${claimed.step_name}"):`,
                err
              );
              await this.stepExecutor.recoverFromWorkerError(
                stepExecId,
                claimed,
                queue
              );
              inflightHandled = true;
            }
          } finally {
            this.inFlight.delete(work);
            this.inFlightStepIds.delete(stepExecId);
            this.inFlightStepNames.delete(stepExecId);
            this.abortControllers.delete(stepExecId);
            this.inFlightRunIds.delete(stepExecId);
          }
        } finally {
          if (!inflightHandled) {
            await this.fairQueue.ack(queue.name, stepExecId);
          }
        }
      } catch (err) {
        this.logger.error(`[chotu] Worker error on queue "${queue.name}":`, err);
        await sleep(1e3);
      }
    }
  }
  async renewInFlightLeases() {
    for (const stepExecId of this.inFlightStepIds) {
      const stepName = this.inFlightStepNames.get(stepExecId);
      if (!stepName) continue;
      await this.stateStore.renewLease(
        stepExecId,
        this.instanceId,
        this.registry.getLeaseTtlMs(stepName)
      );
    }
  }
  async runLeaderRecovery() {
    const steps = [
      async () => {
        await this.recovery.recoverInflightSteps();
      },
      async () => {
        await this.recovery.recoverStaleRunningSteps();
      },
      async () => {
        await this.recovery.recoverOrphanedPendingSteps();
      },
      () => this.recovery.rebuildJoinStateFromRedis()
    ];
    for (const step of steps) {
      if (!await this.stateStore.tryAcquireRecoveryLeader(this.instanceId)) {
        return;
      }
      await step();
    }
  }
  async handleFailedClaim(stepExecId, queueName) {
    const row = await this.stepExecutor.loadStep(stepExecId);
    if (!row) {
      this.logger.warn(`[chotu] Claim failed for missing step ${stepExecId}`);
      await this.fairQueue.ack(queueName, stepExecId);
      return;
    }
    if (row.status === "running" /* RUNNING */) {
      await this.fairQueue.ack(queueName, stepExecId);
      return;
    }
    if (row.status === "pending" /* PENDING */) {
      await this.fairQueue.requeue(queueName, stepExecId, row.workflow_run_id);
      return;
    }
    await this.fairQueue.ack(queueName, stepExecId);
  }
};

// src/logger.ts
var defaultLogger = {
  info: (msg, ...args) => console.log(msg, ...args),
  warn: (msg, ...args) => console.warn(msg, ...args),
  error: (msg, ...args) => console.error(msg, ...args)
};

// src/schema.ts
var WORKFLOW_RUN_STATUS = ["running", "completed", "failed", "cancelled"];
var STEP_STATUS = ["pending", "running", "completed", "failed", "waiting", "cancelled"];
var WORKFLOW_RUN_STATUS_CHECK = WORKFLOW_RUN_STATUS.map((s) => `'${s}'`).join(", ");
var STEP_STATUS_CHECK = STEP_STATUS.map((s) => `'${s}'`).join(", ");
async function migrationApplied(sql, version) {
  const [row] = await sql`
        SELECT 1 FROM chotu.schema_migrations WHERE version = ${version}
    `;
  return Boolean(row);
}
async function recordMigration(sql, version, name) {
  await sql`
        INSERT INTO chotu.schema_migrations (version, name)
        VALUES (${version}, ${name})
        ON CONFLICT (version) DO NOTHING
    `;
}
var migrations = [
  {
    version: 1,
    name: "initial",
    up: async (sql) => {
      await sql.unsafe(`
                CREATE TABLE IF NOT EXISTS chotu.workflow_runs (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    workflow_name TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'running'
                        CHECK (status IN (${WORKFLOW_RUN_STATUS_CHECK})),
                    input JSONB NOT NULL DEFAULT '{}',
                    output JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ
                )
            `);
      await sql.unsafe(`
                CREATE TABLE IF NOT EXISTS chotu.step_executions (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    workflow_run_id UUID NOT NULL REFERENCES chotu.workflow_runs(id),
                    step_name TEXT NOT NULL,
                    queue TEXT NOT NULL DEFAULT 'default',
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN (${STEP_STATUS_CHECK})),
                    input JSONB,
                    output JSONB,
                    error JSONB,
                    join_step_id UUID REFERENCES chotu.step_executions(id),
                    fan_out_index INT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ
                )
            `);
      await sql.unsafe(`
                ALTER TABLE chotu.step_executions
                    ADD COLUMN IF NOT EXISTS queue TEXT DEFAULT 'default'
            `);
      await sql.unsafe(`
                ALTER TABLE chotu.step_executions
                    ADD COLUMN IF NOT EXISTS error JSONB
            `);
      await sql.unsafe(`
                CREATE INDEX IF NOT EXISTS step_executions_workflow_run_id_idx
                    ON chotu.step_executions (workflow_run_id)
            `);
      await sql.unsafe(`
                CREATE INDEX IF NOT EXISTS step_executions_join_step_id_idx
                    ON chotu.step_executions (join_step_id)
            `);
      await sql.unsafe(`
                CREATE INDEX IF NOT EXISTS step_executions_workflow_run_status_idx
                    ON chotu.step_executions (workflow_run_id, status)
            `);
      await sql.unsafe(`
                CREATE INDEX IF NOT EXISTS step_executions_status_updated_at_idx
                    ON chotu.step_executions (status, updated_at)
            `);
    }
  },
  {
    version: 2,
    name: "join_columns",
    up: async (sql) => {
      await sql.unsafe(`
                ALTER TABLE chotu.step_executions
                    ADD COLUMN IF NOT EXISTS join_total INT
            `);
      await sql.unsafe(`
                ALTER TABLE chotu.step_executions
                    ADD COLUMN IF NOT EXISTS join_remaining INT
            `);
    }
  },
  {
    version: 3,
    name: "indexes",
    up: async (sql) => {
      await sql.unsafe(`
                CREATE INDEX IF NOT EXISTS step_executions_run_step_name_idx
                    ON chotu.step_executions (workflow_run_id, step_name)
            `);
      await sql.unsafe(`
                CREATE UNIQUE INDEX IF NOT EXISTS step_executions_active_step_per_run_idx
                    ON chotu.step_executions (workflow_run_id, step_name)
                    WHERE status IN ('pending', 'running', 'waiting')
            `);
    }
  },
  {
    version: 4,
    name: "fk_cascade",
    up: async (sql) => {
      const [constraint] = await sql`
                SELECT con.conname AS name
                FROM pg_constraint con
                JOIN pg_class rel ON rel.oid = con.conrelid
                JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
                WHERE nsp.nspname = 'chotu'
                    AND rel.relname = 'step_executions'
                    AND con.contype = 'f'
                    AND con.confrelid = (
                        SELECT oid FROM pg_class
                        WHERE relname = 'workflow_runs'
                            AND relnamespace = (
                                SELECT oid FROM pg_namespace WHERE nspname = 'chotu'
                            )
                    )
            `;
      if (constraint?.name) {
        await sql.unsafe(
          `ALTER TABLE chotu.step_executions DROP CONSTRAINT "${constraint.name}"`
        );
      }
      await sql.unsafe(`
                ALTER TABLE chotu.step_executions
                    ADD CONSTRAINT step_executions_workflow_run_id_fkey
                    FOREIGN KEY (workflow_run_id)
                    REFERENCES chotu.workflow_runs(id)
                    ON DELETE CASCADE
            `);
    }
  },
  {
    version: 5,
    name: "attempts_and_status_index",
    up: async (sql) => {
      await sql.unsafe(`
                ALTER TABLE chotu.step_executions
                    ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0
            `);
      await sql.unsafe(`
                CREATE INDEX IF NOT EXISTS workflow_runs_status_idx
                    ON chotu.workflow_runs (status)
            `);
    }
  },
  {
    version: 6,
    name: "version_columns",
    up: async (sql) => {
      await sql.unsafe(`
                ALTER TABLE chotu.step_executions
                    ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0
            `);
      await sql.unsafe(`
                ALTER TABLE chotu.workflow_runs
                    ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0
            `);
    }
  },
  {
    version: 7,
    name: "drop_active_step_unique_index",
    up: async (sql) => {
      await sql.unsafe(`
                DROP INDEX IF EXISTS chotu.step_executions_active_step_per_run_idx
            `);
    }
  },
  {
    version: 9,
    name: "active_step_unique_excludes_fanout",
    up: async (sql) => {
      await sql.unsafe(`
                DROP INDEX IF EXISTS chotu.step_executions_active_step_per_run_idx
            `);
      await sql.unsafe(`
                CREATE UNIQUE INDEX step_executions_active_step_per_run_idx
                    ON chotu.step_executions (workflow_run_id, step_name)
                    WHERE status IN ('pending', 'running', 'waiting')
                        AND fan_out_index IS NULL
            `);
    }
  },
  {
    version: 10,
    name: "cancelled_status",
    up: async (sql) => {
      const workflowConstraints = await sql`
                SELECT con.conname AS name
                FROM pg_constraint con
                JOIN pg_class rel ON rel.oid = con.conrelid
                JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
                WHERE nsp.nspname = 'chotu'
                    AND rel.relname = 'workflow_runs'
                    AND con.contype = 'c'
            `;
      for (const row of workflowConstraints) {
        await sql.unsafe(
          `ALTER TABLE chotu.workflow_runs DROP CONSTRAINT "${row.name}"`
        );
      }
      await sql.unsafe(`
                ALTER TABLE chotu.workflow_runs
                    ADD CONSTRAINT workflow_runs_status_check
                    CHECK (status IN (${WORKFLOW_RUN_STATUS_CHECK}))
            `);
      const stepConstraints = await sql`
                SELECT con.conname AS name
                FROM pg_constraint con
                JOIN pg_class rel ON rel.oid = con.conrelid
                JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
                WHERE nsp.nspname = 'chotu'
                    AND rel.relname = 'step_executions'
                    AND con.contype = 'c'
            `;
      for (const row of stepConstraints) {
        await sql.unsafe(
          `ALTER TABLE chotu.step_executions DROP CONSTRAINT "${row.name}"`
        );
      }
      await sql.unsafe(`
                ALTER TABLE chotu.step_executions
                    ADD CONSTRAINT step_executions_status_check
                    CHECK (status IN (${STEP_STATUS_CHECK}))
            `);
    }
  }
];
async function ensureSchema(sql, logger = defaultLogger) {
  await sql`CREATE SCHEMA IF NOT EXISTS chotu`;
  await sql.unsafe(`
        CREATE TABLE IF NOT EXISTS chotu.schema_migrations (
            version INT PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
  for (const migration of migrations) {
    if (await migrationApplied(sql, migration.version)) {
      continue;
    }
    await migration.up(sql);
    await recordMigration(sql, migration.version, migration.name);
    logger.info(`[chotu] Applied schema migration ${migration.version}: ${migration.name}`);
  }
  logger.info("[chotu] Schema ready");
}

// src/domain/execution.mapper.ts
var EMPTY = "";
function parseRedisFields(fields) {
  if (!Array.isArray(fields)) {
    return fields ?? {};
  }
  const out = {};
  for (let i = 0; i < fields.length; i += 2) {
    out[fields[i]] = fields[i + 1] ?? EMPTY;
  }
  return out;
}
function decodeJson(value) {
  if (value == null || value === EMPTY) return null;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}
function parseOptionalInt(value) {
  if (value == null || value === EMPTY || value === "null") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}
function fromRedisHash(hash) {
  if (!hash.id) return null;
  const leaseOwner = hash.lease_owner;
  return {
    id: hash.id,
    workflow_run_id: hash.workflow_run_id ?? "",
    step_name: hash.step_name ?? "",
    queue: hash.queue ?? "default",
    status: hash.status ?? "pending" /* PENDING */,
    input: decodeJson(hash.input),
    output: decodeJson(hash.output),
    error: decodeJson(hash.error),
    join_step_id: hash.join_step_id && hash.join_step_id !== "null" ? hash.join_step_id : null,
    fan_out_index: parseOptionalInt(hash.fan_out_index),
    join_total: parseOptionalInt(hash.join_total),
    join_remaining: parseOptionalInt(hash.join_remaining),
    attempts: Number(hash.attempts ?? 0),
    version: Number(hash.version ?? 0),
    updated_at: hash.updated_at ?? (/* @__PURE__ */ new Date(0)).toISOString(),
    queued: hash.queued === "1",
    lease_owner: leaseOwner && leaseOwner !== "" ? leaseOwner : null,
    lease_until: Number(hash.lease_until ?? 0)
  };
}
function fromRedisRunHash(hash, workflowRunId) {
  if (!hash.id) return null;
  return {
    id: hash.id ?? workflowRunId,
    workflow_name: hash.workflow_name ?? "",
    status: hash.status ?? "running" /* RUNNING */,
    input: decodeJson(hash.input) ?? {},
    output: decodeJson(hash.output),
    active_count: Number(hash.active_count ?? 0),
    version: Number(hash.version ?? 0)
  };
}
function fromPgRow(row) {
  return {
    id: row.id,
    workflowRunId: row.workflow_run_id,
    stepName: row.step_name,
    queue: row.queue ?? "default",
    status: row.status,
    input: row.input,
    output: row.output,
    error: row.error,
    joinStepId: row.join_step_id,
    fanOutIndex: row.fan_out_index,
    attempts: row.attempts ?? 0,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    finishedAt: row.finished_at
  };
}

// src/persistence/pg/flusher.ts
var PEL_RECLAIM_MS = 3e4;
var PgFlusher = class {
  constructor(redis, repository, flushIntervalMs, logger = defaultLogger) {
    this.redis = redis;
    this.repository = repository;
    this.flushIntervalMs = flushIntervalMs;
    this.logger = logger;
  }
  running = false;
  loopPromise = null;
  consumerId = `flusher-${crypto.randomUUID().slice(0, 8)}`;
  async start() {
    if (this.running) return;
    this.running = true;
    try {
      await this.redis.send("XGROUP", [
        "CREATE",
        SYNC_STREAM,
        SYNC_CONSUMER_GROUP,
        "0",
        "MKSTREAM"
      ]);
    } catch {
    }
    this.loopPromise = this.runLoop();
  }
  async stop(drainTimeoutMs = 5e3) {
    this.running = false;
    if (!this.loopPromise) return;
    await Promise.race([
      this.loopPromise,
      sleep(drainTimeoutMs)
    ]);
    this.loopPromise = null;
  }
  async runLoop() {
    while (this.running) {
      try {
        await this.flushBatch();
      } catch (err) {
        this.logger.error("[chotu] Flusher error:", err);
      }
      await sleep(this.flushIntervalMs);
    }
    try {
      await this.flushBatch();
    } catch (err) {
      this.logger.error("[chotu] Flusher drain error:", err);
    }
  }
  async reclaimStalePending() {
    const minIdle = String(PEL_RECLAIM_MS);
    const result = await this.redis.send("XAUTOCLAIM", [
      SYNC_STREAM,
      SYNC_CONSUMER_GROUP,
      this.consumerId,
      minIdle,
      "0-0",
      "COUNT",
      "100"
    ]);
    if (!result) return [];
    return result[1] ?? [];
  }
  async flushBatch() {
    const reclaimed = await this.reclaimStalePending();
    await this.processEntries(reclaimed);
    const result = await this.redis.send("XREADGROUP", [
      "GROUP",
      SYNC_CONSUMER_GROUP,
      this.consumerId,
      "COUNT",
      "100",
      "BLOCK",
      "100",
      "STREAMS",
      SYNC_STREAM,
      ">"
    ]);
    if (!result?.length) return;
    const [, entries] = result[0];
    await this.processEntries(entries);
  }
  async processEntries(entries) {
    const ackIds = [];
    for (const [entryId, flatFields] of entries) {
      const fields = parseRedisFields(flatFields);
      try {
        await this.applyEvent(fields);
        ackIds.push(entryId);
      } catch (err) {
        this.logger.error(`[chotu] Failed to flush event ${entryId}:`, err);
      }
    }
    if (ackIds.length) {
      await this.redis.send("XACK", [SYNC_STREAM, SYNC_CONSUMER_GROUP, ...ackIds]);
      await this.redis.send("XTRIM", [SYNC_STREAM, "MAXLEN", "~", "10000"]);
    }
  }
  async applyEvent(fields) {
    const type = fields.type;
    const version = Number(fields.version ?? 0);
    if (type === "step.status") {
      await this.repository.syncStepStatus({
        id: fields.id,
        status: fields.status,
        updatedAt: fields.updated_at ?? (/* @__PURE__ */ new Date()).toISOString(),
        version
      });
      return;
    }
    if (type === "step.attempts") {
      await this.repository.syncStepAttempts({
        id: fields.id,
        attempts: Number(fields.attempts ?? 0),
        updatedAt: fields.updated_at ?? (/* @__PURE__ */ new Date()).toISOString(),
        version
      });
    }
  }
};

// src/persistence/pg/repository.ts
var PgRepository = class {
  constructor(sql) {
    this.sql = sql;
  }
  async getWorkflowRun(id) {
    const [row] = await this.sql`
            SELECT *
            FROM chotu.workflow_runs
            WHERE id = ${id}
        `;
    if (!row) return null;
    return this.mapWorkflowRun(row);
  }
  async getStepExecutions(workflowRunId) {
    const rows = await this.sql`
            SELECT *
            FROM chotu.step_executions
            WHERE workflow_run_id = ${workflowRunId}
            ORDER BY created_at ASC
        `;
    return rows.map((row) => fromPgRow(row));
  }
  async insertWorkflowRunWithFirstStep(params) {
    const now = /* @__PURE__ */ new Date();
    await this.sql.begin(async (tx) => {
      await tx`
                INSERT INTO chotu.workflow_runs (
                    id,
                    workflow_name,
                    status,
                    input,
                    created_at,
                    updated_at
                ) VALUES (
                    ${params.workflowRunId},
                    ${params.workflowName},
                    ${"running" /* RUNNING */},
                    ${params.input},
                    ${now},
                    ${now}
                )
            `;
      await tx`
                INSERT INTO chotu.step_executions (
                    id,
                    workflow_run_id,
                    step_name,
                    queue,
                    status,
                    input,
                    attempts,
                    created_at,
                    updated_at
                ) VALUES (
                    ${params.firstStepId},
                    ${params.workflowRunId},
                    ${params.firstStepName},
                    ${params.queue},
                    ${"pending" /* PENDING */},
                    ${params.input},
                    0,
                    ${now},
                    ${now}
                )
            `;
    });
  }
  async insertStep(params) {
    const now = /* @__PURE__ */ new Date();
    await this.sql`
            INSERT INTO chotu.step_executions (
                id,
                workflow_run_id,
                step_name,
                queue,
                status,
                input,
                join_step_id,
                fan_out_index,
                join_total,
                join_remaining,
                attempts,
                created_at,
                updated_at
            ) VALUES (
                ${params.id},
                ${params.workflowRunId},
                ${params.stepName},
                ${params.queue},
                ${params.status},
                ${params.input != null ? params.input : null},
                ${params.joinStepId ?? null},
                ${params.fanOutIndex ?? null},
                ${params.joinTotal ?? null},
                ${params.joinRemaining ?? null},
                0,
                ${now},
                ${now}
            )
        `;
  }
  async syncStepTerminal(params) {
    const now = /* @__PURE__ */ new Date();
    if (params.status === "completed" /* COMPLETED */) {
      await this.sql`
                UPDATE chotu.step_executions
                SET status = ${params.status},
                    output = ${params.output ?? null},
                    error = NULL,
                    updated_at = ${now},
                    finished_at = ${now},
                    version = ${params.version}
                WHERE id = ${params.id}
            `;
    } else {
      await this.sql`
                UPDATE chotu.step_executions
                SET status = ${params.status},
                    error = ${params.error ?? null},
                    updated_at = ${now},
                    finished_at = ${now},
                    version = ${params.version}
                WHERE id = ${params.id}
            `;
    }
  }
  async syncJoinFinalize(params) {
    const now = /* @__PURE__ */ new Date();
    await this.sql`
            UPDATE chotu.step_executions
            SET status = ${"pending" /* PENDING */},
                input = ${params.input},
                join_remaining = NULL,
                join_total = NULL,
                updated_at = ${now},
                version = ${params.version}
            WHERE id = ${params.id}
        `;
  }
  async syncJoinRemaining(id, remaining) {
    await this.sql`
            UPDATE chotu.step_executions
            SET join_remaining = ${remaining}, updated_at = NOW()
            WHERE id = ${id}
        `;
  }
  async syncWorkflowTerminal(params) {
    const now = /* @__PURE__ */ new Date();
    const [row] = await this.sql`
            UPDATE chotu.workflow_runs
            SET status = ${params.status},
                output = ${params.output},
                updated_at = ${now},
                finished_at = ${now},
                version = ${params.version}
            WHERE id = ${params.id}
                AND status = ${"running" /* RUNNING */}
            RETURNING id
        `;
    return Boolean(row);
  }
  async syncStepAttempts(params) {
    const [row] = await this.sql`
            UPDATE chotu.step_executions
            SET attempts = ${params.attempts},
                updated_at = ${new Date(params.updatedAt)},
                version = ${params.version}
            WHERE id = ${params.id} AND version <= ${params.version}
            RETURNING id
        `;
    return Boolean(row);
  }
  async syncStepStatus(params) {
    if (params.attempts != null) {
      const [row2] = await this.sql`
                UPDATE chotu.step_executions
                SET status = ${params.status},
                    attempts = ${params.attempts},
                    updated_at = ${new Date(params.updatedAt)},
                    version = ${params.version}
                WHERE id = ${params.id} AND version <= ${params.version}
                RETURNING id
            `;
      return Boolean(row2);
    }
    const [row] = await this.sql`
            UPDATE chotu.step_executions
            SET status = ${params.status},
                updated_at = ${new Date(params.updatedAt)},
                version = ${params.version}
            WHERE id = ${params.id} AND version <= ${params.version}
            RETURNING id
        `;
    return Boolean(row);
  }
  async listPendingSteps() {
    const rows = await this.sql`
            SELECT se.id, se.workflow_run_id, se.queue
            FROM chotu.step_executions se
            INNER JOIN chotu.workflow_runs wr ON wr.id = se.workflow_run_id
            WHERE se.status = ${"pending" /* PENDING */}
                AND wr.status = ${"running" /* RUNNING */}
        `;
    return rows;
  }
  async getRunRow(workflowRunId) {
    const [row] = await this.sql`
            SELECT workflow_name, input, status
            FROM chotu.workflow_runs
            WHERE id = ${workflowRunId}
        `;
    return row ?? null;
  }
  async getCompleteStepRow(workflowRunId, completeStepName) {
    const [row] = await this.sql`
            SELECT id, status, output
            FROM chotu.step_executions
            WHERE workflow_run_id = ${workflowRunId}
                AND step_name = ${completeStepName}
            ORDER BY created_at DESC
            LIMIT 1
        `;
    return row ?? null;
  }
  async insertCompleteStep(params) {
    const now = /* @__PURE__ */ new Date();
    await this.sql`
            INSERT INTO chotu.step_executions (
                id,
                workflow_run_id,
                step_name,
                queue,
                status,
                input,
                attempts,
                created_at,
                updated_at
            ) VALUES (
                ${params.id},
                ${params.workflowRunId},
                ${params.stepName},
                ${params.queue},
                ${"pending" /* PENDING */},
                ${params.input},
                0,
                ${now},
                ${now}
            )
        `;
  }
  async completeWorkflowFromCompleteStep(params) {
    const now = /* @__PURE__ */ new Date();
    const [row] = await this.sql`
            UPDATE chotu.workflow_runs
            SET status = ${"completed" /* COMPLETED */},
                output = ${params.output ?? null},
                updated_at = ${now},
                finished_at = ${now},
                version = COALESCE(${params.version ?? null}, version)
            WHERE id = ${params.workflowRunId}
                AND status = ${"running" /* RUNNING */}
            RETURNING id
        `;
    return Boolean(row);
  }
  async getStepRow(id) {
    const [row] = await this.sql`
            SELECT *
            FROM chotu.step_executions
            WHERE id = ${id}
        `;
    return row ?? null;
  }
  async getRunForHydrate(id) {
    const [row] = await this.sql`
            SELECT *
            FROM chotu.workflow_runs
            WHERE id = ${id}
        `;
    return row ?? null;
  }
  async getTerminalStepOutputs(workflowRunId, terminalNames) {
    const rows = await this.sql`
            SELECT DISTINCT ON (step_name) step_name, output
            FROM chotu.step_executions
            WHERE workflow_run_id = ${workflowRunId}
                AND status = ${"completed" /* COMPLETED */}
            ORDER BY step_name, finished_at DESC NULLS LAST
        `;
    const names = new Set(terminalNames);
    return rows.filter(
      (r) => names.has(r.step_name)
    );
  }
  async deleteStepsForRun(workflowRunId) {
    await this.sql`
            DELETE FROM chotu.step_executions
            WHERE workflow_run_id = ${workflowRunId}
        `;
  }
  mapWorkflowRun(row) {
    return {
      id: row.id,
      workflowName: row.workflow_name,
      status: row.status,
      input: row.input,
      output: row.output,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      finishedAt: row.finished_at
    };
  }
};

// src/persistence/redis/scripts.ts
var FAIR_POP_SCRIPT = `
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
var FAIR_ENQUEUE_SCRIPT = `
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
var REQUEUE_INFLIGHT_SCRIPT = `
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
var ACK_INFLIGHT_SCRIPT = `
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
var RATE_LIMIT_SCRIPT = `
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
var CLAIM_STEP_SCRIPT = `
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
var RENEW_LEASE_SCRIPT = `
local stepKey = KEYS[1]
local leaseOwner = ARGV[1]
local leaseUntil = ARGV[2]
local now = ARGV[3]
if redis.call('HGET', stepKey, 'lease_owner') ~= leaseOwner then return 0 end
if redis.call('HGET', stepKey, 'status') ~= 'running' then return 0 end
redis.call('HSET', stepKey, 'lease_until', leaseUntil, 'updated_at', now)
return 1
`;
var RESET_EXPIRED_LEASE_SCRIPT = `
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
var CREATE_STEP_SCRIPT = `
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
var ROLLBACK_STEP_SCRIPT = `
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
var SET_STEP_STATUS_SCRIPT = `
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
var INCREMENT_ATTEMPTS_SCRIPT = `
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
var DECR_JOIN_SCRIPT = `
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
var COMPLETE_RUN_SCRIPT = `
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
var FAIL_RUN_SCRIPT = `
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
var CANCEL_RUN_SCRIPT = `
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
var CANCEL_FROM_QUEUE_SCRIPT = `
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
var ACQUIRE_RUN_LOCK_SCRIPT = `
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2]) then return 1 end
return 0
`;
var RELEASE_RUN_LOCK_SCRIPT = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;
var ACQUIRE_LEADER_LOCK_SCRIPT = `
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2]) then return 1 end
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('EXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
`;

// src/persistence/redis/fair-queue.ts
var RedisFairQueue = class {
  constructor(redis) {
    this.redis = redis;
  }
  async pop(queueName) {
    return await this.redis.send("EVAL", [
      FAIR_POP_SCRIPT,
      "3",
      queueRotationKey(queueName),
      queueWorkflowsKey(queueName),
      inflightKey(queueName),
      `chotu:queue:${queueName}:wf:`
    ]);
  }
  async ack(queueName, stepExecId) {
    await this.redis.send("EVAL", [
      ACK_INFLIGHT_SCRIPT,
      "2",
      inflightKey(queueName),
      stepKey(stepExecId),
      stepExecId
    ]);
  }
  async requeue(queueName, stepExecId, workflowRunId) {
    await this.redis.send("EVAL", [
      REQUEUE_INFLIGHT_SCRIPT,
      "5",
      inflightKey(queueName),
      queueWfKey(queueName, workflowRunId),
      queueWorkflowsKey(queueName),
      queueRotationKey(queueName),
      stepKey(stepExecId),
      stepExecId,
      workflowRunId
    ]);
  }
  async enqueue(stepExecId, queueName, workflowRunId) {
    await this.redis.send("EVAL", [
      FAIR_ENQUEUE_SCRIPT,
      "4",
      queueWfKey(queueName, workflowRunId),
      queueWorkflowsKey(queueName),
      queueRotationKey(queueName),
      stepKey(stepExecId),
      stepExecId,
      workflowRunId
    ]);
  }
  async enqueueWithRetry(stepExecId, queueName, workflowRunId, maxAttempts = 3) {
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        await this.enqueue(stepExecId, queueName, workflowRunId);
        return;
      } catch (err) {
        if (attempt === maxAttempts - 1) {
          throw err;
        }
        await sleep(100 * (attempt + 1));
      }
    }
  }
  async cancelFromQueue(queueName, stepExecId, workflowRunId) {
    await this.redis.send("EVAL", [
      CANCEL_FROM_QUEUE_SCRIPT,
      "3",
      inflightKey(queueName),
      queueWfKey(queueName, workflowRunId),
      stepKey(stepExecId),
      stepExecId
    ]);
  }
  async acquireRateLimit(queue) {
    if (!queue.rateLimit) return true;
    const windowKey = Math.floor(Date.now() / queue.rateLimit.windowMs);
    const key = rateLimitKey(queue.name, windowKey);
    const allowed = await this.redis.send("EVAL", [
      RATE_LIMIT_SCRIPT,
      "1",
      key,
      String(Math.ceil(queue.rateLimit.windowMs / 1e3)),
      String(queue.rateLimit.max)
    ]);
    return allowed === 1;
  }
  rateLimitBackoffMs(queue) {
    const base = queue.pollIntervalMs ?? 500;
    return base + Math.floor(Math.random() * base);
  }
  async isStepInAnyInflight(stepExecId, queueNames) {
    for (const queueName of queueNames) {
      const pos = await this.redis.send("LPOS", [
        inflightKey(queueName),
        stepExecId
      ]);
      if (pos != null) return true;
    }
    return false;
  }
  async purgeRunFromQueues(workflowRunId, queueNames, stepExecIds) {
    for (const queueName of queueNames) {
      await this.redis.send("DEL", [queueWfKey(queueName, workflowRunId)]);
      await this.redis.send("LREM", [queueRotationKey(queueName), "0", workflowRunId]);
      await this.redis.send("SREM", [queueWorkflowsKey(queueName), workflowRunId]);
      for (const stepExecId of stepExecIds) {
        await this.redis.send("LREM", [inflightKey(queueName), "0", stepExecId]);
      }
    }
  }
};

// src/persistence/redis/state-store.ts
var EMPTY2 = "";
function encodeJson(value) {
  if (value == null) return EMPTY2;
  return JSON.stringify(value);
}
async function readHash(redis, key) {
  const fields = await redis.send("HGETALL", [key]);
  if (!fields || Array.isArray(fields) && fields.length === 0) return {};
  if (!Array.isArray(fields) && Object.keys(fields).length === 0) return {};
  return parseRedisFields(fields);
}
var RedisStateStore = class {
  constructor(redis) {
    this.redis = redis;
  }
  nowIso() {
    return (/* @__PURE__ */ new Date()).toISOString();
  }
  async existsStep(stepExecId) {
    const n = await this.redis.send("EXISTS", [stepKey(stepExecId)]);
    return n === 1;
  }
  async existsRun(workflowRunId) {
    const n = await this.redis.send("EXISTS", [runKey(workflowRunId)]);
    return n === 1;
  }
  async loadStep(stepExecId) {
    const hash = await readHash(this.redis, stepKey(stepExecId));
    if (!hash.id) return null;
    return fromRedisHash(hash);
  }
  async loadRun(workflowRunId) {
    const hash = await readHash(this.redis, runKey(workflowRunId));
    return fromRedisRunHash(hash, workflowRunId);
  }
  async getActiveCount(workflowRunId) {
    const count = await this.redis.send("HGET", [
      runKey(workflowRunId),
      "active_count"
    ]);
    return Number(count ?? 0);
  }
  async claimStep(stepExecId, leaseOwner, leaseTtlMs) {
    const now = this.nowIso();
    const leaseUntil = String(Date.now() + leaseTtlMs);
    const result = await this.redis.send("EVAL", [
      CLAIM_STEP_SCRIPT,
      "2",
      stepKey(stepExecId),
      SYNC_STREAM,
      now,
      leaseOwner,
      leaseUntil,
      stepExecId
    ]);
    if (!result?.length) return null;
    return fromRedisHash(parseRedisFields(result));
  }
  async renewLease(stepExecId, leaseOwner, leaseTtlMs) {
    const result = await this.redis.send("EVAL", [
      RENEW_LEASE_SCRIPT,
      "1",
      stepKey(stepExecId),
      leaseOwner,
      String(Date.now() + leaseTtlMs),
      this.nowIso()
    ]);
    return result === 1;
  }
  async resetExpiredLease(stepExecId) {
    const result = await this.redis.send("EVAL", [
      RESET_EXPIRED_LEASE_SCRIPT,
      "2",
      stepKey(stepExecId),
      SYNC_STREAM,
      String(Date.now()),
      this.nowIso(),
      stepExecId
    ]);
    return result === 1;
  }
  async setStepStatus(stepExecId, status) {
    const row = await this.loadStep(stepExecId);
    if (!row) return false;
    const result = await this.redis.send("EVAL", [
      SET_STEP_STATUS_SCRIPT,
      "4",
      stepKey(stepExecId),
      runKey(row.workflow_run_id),
      activeStepKey(row.workflow_run_id, row.step_name),
      SYNC_STREAM,
      status,
      this.nowIso(),
      stepExecId
    ]);
    return result === 1;
  }
  async incrementAttempts(stepExecId) {
    const result = await this.redis.send("EVAL", [
      INCREMENT_ATTEMPTS_SCRIPT,
      "2",
      stepKey(stepExecId),
      SYNC_STREAM,
      this.nowIso(),
      stepExecId
    ]);
    return result ?? 0;
  }
  async decrementJoinRemaining(joinStepId) {
    const result = await this.redis.send("EVAL", [
      DECR_JOIN_SCRIPT,
      "1",
      stepKey(joinStepId),
      this.nowIso()
    ]);
    return result;
  }
  async rollbackStep(stepExecId, workflowRunId, stepName) {
    await this.redis.send("EVAL", [
      ROLLBACK_STEP_SCRIPT,
      "3",
      stepKey(stepExecId),
      runKey(workflowRunId),
      activeStepKey(workflowRunId, stepName),
      stepExecId
    ]);
    await this.redis.send("SREM", [runStepsKey(workflowRunId), stepExecId]);
  }
  async rollbackRun(workflowRunId) {
    await this.purgeRunKeys(workflowRunId);
  }
  async purgeRun(workflowRunId, stepExecIds, joinBranchKeys) {
    if (stepExecIds.length) {
      await this.redis.send("DEL", stepExecIds.map((id) => stepKey(id)));
    }
    if (joinBranchKeys.length) {
      await this.redis.send("DEL", joinBranchKeys);
    }
    await this.purgeRunKeys(workflowRunId);
  }
  async purgeRunKeys(workflowRunId) {
    await this.redis.send("DEL", [
      runKey(workflowRunId),
      runStepsKey(workflowRunId),
      runLockKey(workflowRunId)
    ]);
    const prefix = `chotu:run:${workflowRunId}:`;
    let cursor = "0";
    do {
      const [nextCursor, keys] = await this.redis.send("SCAN", [
        cursor,
        "MATCH",
        `${prefix}*`,
        "COUNT",
        "100"
      ]);
      cursor = nextCursor;
      if (keys.length) await this.redis.send("DEL", keys);
    } while (cursor !== "0");
  }
  async createRun(params) {
    const now = this.nowIso();
    await this.redis.send("HSET", [
      runKey(params.id),
      "id",
      params.id,
      "workflow_name",
      params.workflowName,
      "status",
      "running" /* RUNNING */,
      "input",
      encodeJson(params.input),
      "output",
      EMPTY2,
      "active_count",
      "0",
      "version",
      "0",
      "created_at",
      now,
      "updated_at",
      now
    ]);
  }
  async createStep(params) {
    const status = params.status ?? "pending" /* PENDING */;
    const now = this.nowIso();
    const joinStepId = params.joinStepId ?? null;
    const result = await this.redis.send("EVAL", [
      CREATE_STEP_SCRIPT,
      "4",
      stepKey(params.id),
      runKey(params.workflowRunId),
      activeStepKey(params.workflowRunId, params.stepName),
      joinStepId ? joinBranchesKey(joinStepId) : stepKey("_noop_branches"),
      params.id,
      params.workflowRunId,
      params.stepName,
      params.queue,
      status,
      encodeJson(params.input),
      joinStepId ?? "null",
      params.fanOutIndex != null ? String(params.fanOutIndex) : "null",
      params.joinTotal != null ? String(params.joinTotal) : "null",
      params.joinRemaining != null ? String(params.joinRemaining) : "null",
      "0",
      now,
      now
    ]);
    if (result === 1) {
      await this.redis.send("SADD", [runStepsKey(params.workflowRunId), params.id]);
    }
    return result === 1;
  }
  async completeStep(stepExecId, output) {
    const row = await this.loadStep(stepExecId);
    if (!row) return null;
    const now = this.nowIso();
    await this.applyTerminalTransition(row, "completed" /* COMPLETED */, now, {
      output: encodeJson(output),
      error: EMPTY2,
      finished_at: now
    });
    return this.loadStep(stepExecId);
  }
  async failStep(stepExecId, error) {
    const row = await this.loadStep(stepExecId);
    if (!row) return null;
    const now = this.nowIso();
    await this.applyTerminalTransition(row, "failed" /* FAILED */, now, {
      output: EMPTY2,
      error: encodeJson(error),
      finished_at: now
    });
    return this.loadStep(stepExecId);
  }
  async cancelStep(stepExecId, reason) {
    const row = await this.loadStep(stepExecId);
    if (!row) return null;
    const now = this.nowIso();
    await this.applyTerminalTransition(row, "cancelled" /* CANCELLED */, now, {
      output: EMPTY2,
      error: encodeJson(reason ? { reason } : null),
      finished_at: now
    });
    return this.loadStep(stepExecId);
  }
  async applyTerminalTransition(row, status, now, extra) {
    if (row.status === "completed" /* COMPLETED */ || row.status === "failed" /* FAILED */ || row.status === "cancelled" /* CANCELLED */) {
      return;
    }
    const activeStatuses = /* @__PURE__ */ new Set([
      "pending" /* PENDING */,
      "running" /* RUNNING */,
      "waiting" /* WAITING */
    ]);
    if (activeStatuses.has(row.status)) {
      await this.redis.send("SREM", [activeStepKey(row.workflow_run_id, row.step_name), row.id]);
      await this.redis.send("HINCRBY", [runKey(row.workflow_run_id), "active_count", "-1"]);
    }
    const version = String(row.version + 1);
    await this.redis.send("HSET", [
      stepKey(row.id),
      "status",
      status,
      "updated_at",
      now,
      "version",
      version,
      "queued",
      "0",
      "lease_owner",
      "",
      "lease_until",
      "0",
      ...Object.entries(extra).flat()
    ]);
  }
  async finalizeJoinStep(joinStepId, input) {
    const row = await this.loadStep(joinStepId);
    if (!row) return null;
    const now = this.nowIso();
    const wasWaiting = row.status === "waiting" /* WAITING */;
    if (wasWaiting) {
      await this.redis.send("SREM", [activeStepKey(row.workflow_run_id, row.step_name), joinStepId]);
    }
    const version = String(row.version + 1);
    await this.redis.send("HSET", [
      stepKey(joinStepId),
      "status",
      "pending" /* PENDING */,
      "input",
      encodeJson(input),
      "join_remaining",
      "null",
      "join_total",
      "null",
      "queued",
      "0",
      "updated_at",
      now,
      "version",
      version
    ]);
    if (wasWaiting) {
      await this.redis.send("SADD", [activeStepKey(row.workflow_run_id, row.step_name), joinStepId]);
    } else {
      await this.redis.send("HINCRBY", [runKey(row.workflow_run_id), "active_count", "1"]);
      await this.redis.send("SADD", [activeStepKey(row.workflow_run_id, row.step_name), joinStepId]);
    }
    return this.loadStep(joinStepId);
  }
  async setJoinRemaining(joinStepId, remaining) {
    await this.redis.send("HSET", [
      stepKey(joinStepId),
      "join_remaining",
      String(remaining),
      "updated_at",
      this.nowIso()
    ]);
  }
  async rebuildJoinRemainingFromBranches(joinStepId) {
    const joinRow = await this.loadStep(joinStepId);
    if (!joinRow || joinRow.status !== "waiting" /* WAITING */) return null;
    if (joinRow.join_remaining != null) return joinRow.join_remaining;
    const branches = await this.getJoinBranches(joinStepId);
    let remaining = 0;
    for (const branch of branches) {
      if (branch.status === "pending" /* PENDING */ || branch.status === "running" /* RUNNING */) {
        remaining++;
      }
    }
    await this.setJoinRemaining(joinStepId, remaining);
    return remaining;
  }
  async getJoinBranches(joinStepId) {
    const branchIds = await this.redis.send("LRANGE", [
      joinBranchesKey(joinStepId),
      "0",
      "-1"
    ]);
    const rows = [];
    for (const id of branchIds ?? []) {
      const row = await this.loadStep(id);
      if (row) rows.push(row);
    }
    return rows.sort((a, b) => (a.fan_out_index ?? 0) - (b.fan_out_index ?? 0));
  }
  async getRunStatus(workflowRunId) {
    const status = await this.redis.send("HGET", [
      runKey(workflowRunId),
      "status"
    ]);
    return status ?? null;
  }
  async getStepsForRun(workflowRunId) {
    const ids = await this.redis.send("SMEMBERS", [runStepsKey(workflowRunId)]);
    const rows = [];
    for (const id of ids ?? []) {
      const row = await this.loadStep(id);
      if (row) rows.push(row);
    }
    return rows;
  }
  async listStepsForRun(workflowRunId) {
    return this.getStepsForRun(workflowRunId);
  }
  async markAbortRequested(workflowRunId) {
    await this.redis.send("HSET", [runKey(workflowRunId), "abort_requested", "1"]);
  }
  async isAbortRequested(workflowRunId) {
    const flag = await this.redis.send("HGET", [
      runKey(workflowRunId),
      "abort_requested"
    ]);
    return flag === "1";
  }
  async recomputeRunActiveCount(workflowRunId) {
    const steps = await this.getStepsForRun(workflowRunId);
    const activeStatuses = /* @__PURE__ */ new Set([
      "pending" /* PENDING */,
      "running" /* RUNNING */,
      "waiting" /* WAITING */
    ]);
    let count = 0;
    for (const step of steps) {
      if (activeStatuses.has(step.status)) count++;
    }
    await this.redis.send("HSET", [runKey(workflowRunId), "active_count", String(count)]);
    return count;
  }
  async findStepByName(workflowRunId, stepName) {
    const steps = await this.getStepsForRun(workflowRunId);
    let latest = null;
    for (const step of steps) {
      if (step.step_name !== stepName) continue;
      if (!latest || step.updated_at > latest.updated_at) {
        latest = step;
      }
    }
    return latest;
  }
  async countUnabsorbedFailures(workflowRunId) {
    const steps = await this.getStepsForRun(workflowRunId);
    let count = 0;
    for (const step of steps) {
      if (step.status !== "failed" /* FAILED */) continue;
      if (step.join_step_id) {
        const join = await this.loadStep(step.join_step_id);
        if (join?.status === "completed" /* COMPLETED */) continue;
      }
      count++;
    }
    return count;
  }
  async tryCompleteRun(workflowRunId, output) {
    const result = await this.redis.send("EVAL", [
      COMPLETE_RUN_SCRIPT,
      "1",
      runKey(workflowRunId),
      this.nowIso(),
      encodeJson(output)
    ]);
    return result > 0 ? result : null;
  }
  async tryFailRun(workflowRunId, reason) {
    const output = encodeJson(reason ? { reason } : null);
    const result = await this.redis.send("EVAL", [
      FAIL_RUN_SCRIPT,
      "1",
      runKey(workflowRunId),
      this.nowIso(),
      output
    ]);
    return result > 0 ? result : null;
  }
  async tryCancelRun(workflowRunId, reason) {
    const output = encodeJson(reason ? { reason } : null);
    const result = await this.redis.send("EVAL", [
      CANCEL_RUN_SCRIPT,
      "1",
      runKey(workflowRunId),
      this.nowIso(),
      output
    ]);
    return result > 0 ? result : null;
  }
  async acquireRunLock(workflowRunId, token, ttlSec = 30) {
    const result = await this.redis.send("EVAL", [
      ACQUIRE_RUN_LOCK_SCRIPT,
      "1",
      runLockKey(workflowRunId),
      token,
      String(ttlSec)
    ]);
    return result === 1;
  }
  async releaseRunLock(workflowRunId, token) {
    await this.redis.send("EVAL", [
      RELEASE_RUN_LOCK_SCRIPT,
      "1",
      runLockKey(workflowRunId),
      token
    ]);
  }
  async tryAcquireRecoveryLeader(instanceId) {
    const result = await this.redis.send("EVAL", [
      ACQUIRE_LEADER_LOCK_SCRIPT,
      "1",
      RECOVERY_LEADER_KEY,
      instanceId,
      String(RECOVERY_LEADER_TTL_SEC)
    ]);
    return result === 1;
  }
  async tryAcquireStartupReconcile(instanceId) {
    const result = await this.redis.send("EVAL", [
      ACQUIRE_LEADER_LOCK_SCRIPT,
      "1",
      STARTUP_RECONCILE_KEY,
      instanceId,
      String(STARTUP_RECONCILE_TTL_SEC)
    ]);
    return result === 1;
  }
  async hydrateRunIfMissing(row) {
    const id = row.id;
    if (await this.existsRun(id)) return false;
    await this.hydrateRun(row);
    return true;
  }
  async hydrateStepIfMissing(row) {
    const id = row.id;
    if (await this.existsStep(id)) return false;
    await this.hydrateStep(row);
    return true;
  }
  async hydrateRun(row) {
    const id = row.id;
    const now = row.updated_at?.toISOString?.() ?? this.nowIso();
    await this.redis.send("HSET", [
      runKey(id),
      "id",
      id,
      "workflow_name",
      row.workflow_name,
      "status",
      row.status,
      "input",
      encodeJson(row.input),
      "output",
      encodeJson(row.output),
      "active_count",
      "0",
      "version",
      String(row.version ?? 0),
      "created_at",
      row.created_at?.toISOString?.() ?? now,
      "updated_at",
      now
    ]);
  }
  async hydrateStep(row) {
    const id = row.id;
    const workflowRunId = row.workflow_run_id;
    const stepName = row.step_name;
    const status = row.status;
    const now = row.updated_at?.toISOString?.() ?? this.nowIso();
    await this.redis.send("HSET", [
      stepKey(id),
      "id",
      id,
      "workflow_run_id",
      workflowRunId,
      "step_name",
      stepName,
      "queue",
      row.queue ?? "default",
      "status",
      status,
      "input",
      encodeJson(row.input),
      "output",
      encodeJson(row.output),
      "error",
      encodeJson(row.error),
      "join_step_id",
      row.join_step_id ? String(row.join_step_id) : "null",
      "fan_out_index",
      row.fan_out_index != null ? String(row.fan_out_index) : "null",
      "join_total",
      row.join_total != null ? String(row.join_total) : "null",
      "join_remaining",
      row.join_remaining != null ? String(row.join_remaining) : "null",
      "attempts",
      String(row.attempts ?? 0),
      "queued",
      "0",
      "lease_owner",
      "",
      "lease_until",
      "0",
      "version",
      String(row.version ?? 0),
      "created_at",
      row.created_at?.toISOString?.() ?? now,
      "updated_at",
      now
    ]);
    const activeStatuses = /* @__PURE__ */ new Set([
      "pending" /* PENDING */,
      "running" /* RUNNING */,
      "waiting" /* WAITING */
    ]);
    if (activeStatuses.has(status)) {
      await this.redis.send("SADD", [activeStepKey(workflowRunId, stepName), id]);
    }
    const joinStepId = row.join_step_id;
    if (joinStepId) {
      await this.redis.send("RPUSH", [joinBranchesKey(joinStepId), id]);
    }
    await this.redis.send("SADD", [runStepsKey(workflowRunId), id]);
  }
  async scanStepIds(pattern) {
    const ids = [];
    let cursor = "0";
    do {
      const [nextCursor, keys] = await this.redis.send("SCAN", [
        cursor,
        "MATCH",
        pattern,
        "COUNT",
        "100"
      ]);
      cursor = nextCursor;
      for (const key of keys) {
        const prefix = "chotu:step:";
        if (key.startsWith(prefix)) {
          const id = key.slice(prefix.length);
          if (id.startsWith("_")) continue;
          ids.push(id);
        }
      }
    } while (cursor !== "0");
    return ids;
  }
};

// src/chotu.impl.ts
var ChotuImpl = class {
  constructor(config, onShutdown) {
    this.config = config;
    this.onShutdown = onShutdown;
    this.logger = config.logger ?? defaultLogger;
    this.sql = createSql(config.postgresUrl, {
      max: config.postgresMaxConnections ?? 10
    });
    this.redis = createRedis(config.redisUrl);
    this.repository = new PgRepository(this.sql);
    this.stateStore = new RedisStateStore(this.redis);
    this.fairQueue = new RedisFairQueue(this.redis);
    this.flusher = new PgFlusher(
      this.redis,
      this.repository,
      config.flushIntervalMs ?? 1e3,
      this.logger
    );
    const registry = new StepRegistry(
      config.queues,
      config.stepQueues,
      config.workflows,
      {
        defaultStepTimeoutMs: config.defaultStepTimeoutMs,
        leaseBufferMs: config.leaseBufferMs
      }
    );
    const hookRunner = new ChotuHookRunner(config.hooks, this.logger);
    const runPurger = new RunPurger(
      this.stateStore,
      this.repository,
      this.fairQueue,
      registry,
      this.logger,
      config.purgeOnTerminal !== false
    );
    const lifecycle = new WorkflowLifecycle(
      this.stateStore,
      this.repository,
      this.fairQueue,
      registry,
      this.logger,
      hookRunner,
      runPurger
    );
    const stepExecutor = new StepExecutor(
      lifecycle,
      registry,
      this.fairQueue,
      this.logger,
      hookRunner,
      config.resolveStep
    );
    const recovery = new RecoveryService(
      this.stateStore,
      this.repository,
      this.fairQueue,
      lifecycle,
      registry,
      this.logger,
      this.redis,
      this.instanceId
    );
    const workerPool = new QueueWorkerPool(
      this.fairQueue,
      this.stateStore,
      stepExecutor,
      recovery,
      registry,
      this.logger,
      this.instanceId,
      hookRunner
    );
    this.engine = new ChotuEngine(
      this.flusher,
      workerPool,
      lifecycle,
      recovery
    );
  }
  sql;
  redis;
  repository;
  stateStore;
  fairQueue;
  flusher;
  engine;
  logger;
  instanceId = `chotu-${crypto.randomUUID().slice(0, 8)}`;
  started = false;
  workersStarted = false;
  isStarted() {
    return this.started;
  }
  areWorkersStarted() {
    return this.workersStarted;
  }
  assertStarted() {
    if (!this.started) {
      throw new Error("[chotu] Call listen() before running workflows");
    }
  }
  async listen(options) {
    if (this.started) return;
    this.started = true;
    this.logger.info("[chotu] Hello from chotu!");
    await this.sql`SELECT 1`;
    this.logger.info("[chotu] Postgres connected");
    await ensureSchema(this.sql, this.logger);
    await this.redis.connect();
    await this.redis.ping();
    this.logger.info("[chotu] Redis connected");
    await this.engine.recoverOnStartup();
    if (!options?.deferWorkers) {
      await this.startWorkers();
    }
  }
  async startWorkers() {
    if (this.workersStarted) return;
    this.workersStarted = true;
    this.engine.setWorkersStarted(true);
    await this.engine.start();
    this.logger.info("[chotu] Ready.");
  }
  async shutdown() {
    if (!this.started) return;
    await this.engine.stop();
    this.started = false;
    this.workersStarted = false;
    this.engine.setWorkersStarted(false);
    await this.sql.end({ timeout: 5 });
    this.redis.close();
    this.logger.info("[chotu] Stopped.");
    this.onShutdown?.();
  }
  async runWorkflow(name, input) {
    this.assertStarted();
    return this.engine.runWorkflow(name, input);
  }
  async getWorkflowRun(id) {
    this.assertStarted();
    return this.engine.getWorkflowRun(id);
  }
  async getStepExecutions(workflowRunId) {
    this.assertStarted();
    return this.engine.getStepExecutions(workflowRunId);
  }
  async abortWorkflow(workflowRunId, reason) {
    this.assertStarted();
    return this.engine.abortWorkflow(workflowRunId, reason);
  }
  async health() {
    let postgres = false;
    let redis = false;
    try {
      await this.sql`SELECT 1`;
      postgres = true;
    } catch {
      postgres = false;
    }
    try {
      await this.redis.connect();
      await this.redis.ping();
      redis = true;
    } catch {
      redis = false;
    }
    return {
      postgres,
      redis,
      workers: this.workersStarted && this.engine.areWorkersStarted()
    };
  }
};

// src/index.ts
var instance;
function resetChotu() {
  instance = void 0;
}
function createChotu(config) {
  if (instance?.isStarted()) {
    throw new Error("[chotu] Already started; call shutdown() first");
  }
  if (instance) {
    resetChotu();
  }
  instance = new ChotuImpl(config, resetChotu);
  return instance;
}
function getChotu() {
  return instance;
}
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  DEFAULT_LEASE_BUFFER_MS,
  DEFAULT_STEP_TIMEOUT_MS,
  FAIR_ENQUEUE_SCRIPT,
  Step,
  StepExecutionStatus,
  StepRegistry,
  Workflow,
  WorkflowRunStatus,
  computeLeaseTtlMs,
  createChotu,
  createStepError,
  defineWorkflow,
  getChotu,
  getStepName,
  inflightKey,
  isChotuStepError,
  isNextStep,
  isParallelSpec,
  next,
  parallel,
  queueRotationKey,
  queueWfKey,
  queueWorkflowsKey,
  resetChotu,
  resolveStepTimeoutMs,
  stepKey,
  validateConfig,
  validateStepQueues
});
