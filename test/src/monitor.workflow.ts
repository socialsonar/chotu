import { Step, isChotuStepError, next, parallel, defineWorkflow, Workflow, type NextStepsResult } from "chotu";

// --- Types passed between steps ---

interface MonitorInput {
    url: string;
    failPlatform?: string;
}

interface SearchResult {
    query: string;
    keywords: string[];
}

interface FetchInput {
    query: string;
    platform: string;
    failPlatform?: string;
}

export interface FetchResult {
    platform: string;
    urls: string[];
}

interface FinalResult {
    combined: string[];
    errors: string[];
}

// --- Steps ---

class SearchStep extends Step<MonitorInput, SearchResult> {
    async run(input: MonitorInput, _signal: AbortSignal): Promise<SearchResult> {
        return {
            query: input.url,
            keywords: ["brand", "product"],
        };
    }

    getNextSteps(input: MonitorInput, output: SearchResult, _signal: AbortSignal) {
        return parallel(
            [
                next(GoogleFetchStep, {
                    query: output.query,
                    platform: "google",
                    failPlatform: input.failPlatform,
                }),
                next(BingFetchStep, {
                    query: output.query,
                    platform: "bing",
                    failPlatform: input.failPlatform,
                }),
            ],
            AggregateStep,
        );
    }
}

class GoogleFetchStep extends Step<FetchInput, FetchResult> {
    async run(input: FetchInput, _signal: AbortSignal): Promise<FetchResult> {
        if (input.failPlatform === input.platform) {
            throw new Error(`Simulated failure for ${input.platform}`);
        }
        return {
            platform: input.platform,
            urls: [`https://google.com/search?q=${input.query}`],
        };
    }

    getNextSteps(_input: FetchInput, _output: FetchResult, _signal: AbortSignal): NextStepsResult {
        return "END";
    }
}

class BingFetchStep extends Step<FetchInput, FetchResult> {
    async run(input: FetchInput, _signal: AbortSignal): Promise<FetchResult> {
        if (input.failPlatform === input.platform) {
            throw new Error(`Simulated failure for ${input.platform}`);
        }
        return {
            platform: input.platform,
            urls: [`https://bing.com/search?q=${input.query}`],
        };
    }

    getNextSteps(_input: FetchInput, _output: FetchResult, _signal: AbortSignal): NextStepsResult {
        return "END";
    }
}

class AggregateStep extends Step<(FetchResult | import("chotu").ChotuStepError)[], FinalResult> {
    async run(
        inputs: (FetchResult | import("chotu").ChotuStepError)[],
        _signal: AbortSignal,
    ): Promise<FinalResult> {
        const combined: string[] = [];
        const errors: string[] = [];

        for (const item of inputs) {
            if (isChotuStepError(item)) {
                errors.push(item.__chotuError.message);
                continue;
            }
            combined.push(...item.urls);
        }

        return { combined, errors };
    }

    getNextSteps(
        _input: (FetchResult | import("chotu").ChotuStepError)[],
        _output: FinalResult,
        _signal: AbortSignal,
    ): NextStepsResult {
        return "END";
    }
}

class FailStep extends Step<MonitorInput, never> {
    async run(_input: MonitorInput, _signal: AbortSignal): Promise<never> {
        throw new Error("Intentional linear failure");
    }

    getNextSteps(_input: MonitorInput, _output: never, _signal: AbortSignal): NextStepsResult {
        return "END";
    }
}

// --- Workflow definitions ---

class MonitorWorkflowClass extends Workflow<MonitorInput, FinalResult> {
    readonly name = "monitor";
    readonly firstStep = SearchStep;
    readonly steps = [SearchStep, GoogleFetchStep, BingFetchStep, AggregateStep];
    readonly terminalSteps = [AggregateStep];
}

class FailWorkflowClass extends Workflow<MonitorInput, never> {
    readonly name = "fail";
    readonly firstStep = FailStep;
    readonly steps = [FailStep];
    readonly terminalSteps = [FailStep];
}

export const MonitorWorkflow = defineWorkflow(MonitorWorkflowClass);
export const FailWorkflow = defineWorkflow(FailWorkflowClass);
