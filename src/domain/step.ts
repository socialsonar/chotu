import type { StepHookContext } from "../interfaces/hooks.interface";

export type StepClass<I, O> = new () => Step<I, O>;

export interface ChotuStepError {
    __chotuError: {
        message: string;
        stepName: string;
    };
}

export function isChotuStepError(value: unknown): value is ChotuStepError {
    return (
        typeof value === "object" &&
        value !== null &&
        "__chotuError" in value &&
        typeof (value as ChotuStepError).__chotuError?.message === "string"
    );
}

export function createStepError(message: string, stepName: string): ChotuStepError {
    return { __chotuError: { message, stepName } };
}

export function getStepName(cls: StepClass<any, any>): string {
    const named = cls as StepClass<any, any> & { stepName?: string };
    return named.stepName ?? cls.name;
}

export function getStepTimeoutMs(cls: StepClass<any, any>): number | undefined {
    const timeoutMs = (cls as StepClass<any, any> & { timeoutMs?: number }).timeoutMs;
    return timeoutMs;
}

export interface NextStep<I> {
    step: StepClass<I, any>;
    input: I;
}

export interface ParallelSpec {
    type: "parallel";
    branches: NextStep<any>[];
    join?: StepClass<any[], any>;
}

export type NextStepsResult = "END" | NextStep<any> | ParallelSpec;

export abstract class Step<I, O> {
    static readonly stepName?: string;
    static readonly timeoutMs?: number;

    abstract run(input: I, signal: AbortSignal): Promise<O>;
    abstract getNextSteps(
        input: I,
        output: O,
        signal: AbortSignal,
    ): NextStepsResult | Promise<NextStepsResult>;

    async onBeforeRun(_input: I, _ctx: StepHookContext, _signal: AbortSignal): Promise<void> {}
    async onAfterRun(
        _input: I,
        _output: O,
        _ctx: StepHookContext,
        _signal: AbortSignal,
    ): Promise<void> {}
    async onError(
        _input: I,
        _error: Error,
        _ctx: StepHookContext,
        _signal: AbortSignal,
    ): Promise<void> {}
}

export function next<I>(step: StepClass<I, any>, input: I): NextStep<I> {
    return { step, input };
}

export function parallel(
    branches: NextStep<any>[],
    join?: StepClass<any[], any>,
): ParallelSpec {
    return { type: "parallel", branches, join };
}

export function isParallelSpec(value: NextStepsResult): value is ParallelSpec {
    return typeof value === "object" && value !== null && "type" in value && value.type === "parallel";
}

export function isNextStep(value: NextStepsResult): value is NextStep<any> {
    return typeof value === "object" && value !== null && "step" in value && "input" in value;
}
