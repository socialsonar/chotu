export var WorkflowRunStatus;
(function (WorkflowRunStatus) {
    WorkflowRunStatus["RUNNING"] = "running";
    WorkflowRunStatus["COMPLETED"] = "completed";
    WorkflowRunStatus["FAILED"] = "failed";
    WorkflowRunStatus["CANCELLED"] = "cancelled";
})(WorkflowRunStatus || (WorkflowRunStatus = {}));
export var StepExecutionStatus;
(function (StepExecutionStatus) {
    StepExecutionStatus["PENDING"] = "pending";
    StepExecutionStatus["RUNNING"] = "running";
    StepExecutionStatus["COMPLETED"] = "completed";
    StepExecutionStatus["FAILED"] = "failed";
    StepExecutionStatus["WAITING"] = "waiting";
    StepExecutionStatus["CANCELLED"] = "cancelled";
})(StepExecutionStatus || (StepExecutionStatus = {}));
//# sourceMappingURL=workflow.interface.js.map