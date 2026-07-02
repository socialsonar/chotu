export type StepClass<I, O> = new () => Step<I, O>;
export interface ChotuStepError {
    __chotuError: {
        message: string;
        stepName: string;
    };
}
export declare function isChotuStepError(value: unknown): value is ChotuStepError;
export declare function createStepError(message: string, stepName: string): ChotuStepError;
export declare function getStepName(cls: StepClass<any, any>): string;
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
export declare abstract class Step<I, O> {
    static readonly stepName?: string;
    abstract run(input: I, signal: AbortSignal): Promise<O>;
    abstract getNextSteps(input: I, output: O, signal: AbortSignal): NextStepsResult | Promise<NextStepsResult>;
    onBeforeRun(_input: I, _signal: AbortSignal): Promise<void>;
    onAfterRun(_input: I, _output: O, _signal: AbortSignal): Promise<void>;
    onError(_input: I, _error: Error, _signal: AbortSignal): Promise<void>;
}
export declare function next<I>(step: StepClass<I, any>, input: I): NextStep<I>;
export declare function parallel(branches: NextStep<any>[], join?: StepClass<any[], any>): ParallelSpec;
export declare function isParallelSpec(value: NextStepsResult): value is ParallelSpec;
export declare function isNextStep(value: NextStepsResult): value is NextStep<any>;
//# sourceMappingURL=step.d.ts.map