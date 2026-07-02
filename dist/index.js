import ChotuImpl from "./chotu.impl";
import {
  StepExecutionStatus,
  WorkflowRunStatus
} from "./interfaces/workflow.interface";
import {
  Step,
  createStepError,
  getStepName,
  isChotuStepError,
  next,
  parallel,
  isNextStep,
  isParallelSpec
} from "./domain/step";
import { StepRegistry } from "./engine/step-registry";
import {
  computeLeaseTtlMs,
  DEFAULT_LEASE_BUFFER_MS,
  DEFAULT_STEP_TIMEOUT_MS,
  resolveStepTimeoutMs
} from "./domain/timeout";
import { defineWorkflow, validateConfig, validateStepQueues } from "./domain/workflow";
import { FAIR_ENQUEUE_SCRIPT } from "./persistence/redis/scripts";
import {
  queueRotationKey,
  queueWfKey,
  queueWorkflowsKey,
  stepKey,
  inflightKey
} from "./persistence/redis/keys";
let instance;
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
export {
  DEFAULT_LEASE_BUFFER_MS,
  DEFAULT_STEP_TIMEOUT_MS,
  FAIR_ENQUEUE_SCRIPT,
  Step,
  StepExecutionStatus,
  StepRegistry,
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
};
