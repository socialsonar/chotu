export function isChotuStepError(value) {
    return (typeof value === "object" &&
        value !== null &&
        "__chotuError" in value &&
        typeof value.__chotuError?.message === "string");
}
export function createStepError(message, stepName) {
    return { __chotuError: { message, stepName } };
}
export function getStepName(cls) {
    const named = cls;
    return named.stepName ?? cls.name;
}
export function getStepTimeoutMs(cls) {
    const timeoutMs = cls.timeoutMs;
    return timeoutMs;
}
export class Step {
    static stepName;
    static timeoutMs;
    async onBeforeRun(_input, _ctx, _signal) { }
    async onAfterRun(_input, _output, _ctx, _signal) { }
    async onError(_input, _error, _ctx, _signal) { }
}
export function next(step, input) {
    return { step, input };
}
export function parallel(branches, join) {
    return { type: "parallel", branches, join };
}
export function isParallelSpec(value) {
    return typeof value === "object" && value !== null && "type" in value && value.type === "parallel";
}
export function isNextStep(value) {
    return typeof value === "object" && value !== null && "step" in value && "input" in value;
}
//# sourceMappingURL=step.js.map