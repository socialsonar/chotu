import { describe, expect, test } from "bun:test";
import {
    computeLeaseTtlMs,
    DEFAULT_LEASE_BUFFER_MS,
    DEFAULT_STEP_TIMEOUT_MS,
    defineWorkflow,
    resolveStepTimeoutMs,
    Step,
    StepRegistry,
    Workflow,
} from "chotu";

class ShortStep extends Step<{ v: number }, { done: true }> {
    static stepName = "ShortStep";
    static timeoutMs = 5_000;

    async run() {
        return { done: true as const };
    }

    getNextSteps() {
        return "END" as const;
    }
}

class DefaultTimeoutStep extends Step<{ v: number }, { done: true }> {
    static stepName = "DefaultTimeoutStep";

    async run() {
        return { done: true as const };
    }

    getNextSteps() {
        return "END" as const;
    }
}

class TimeoutLeaseWorkflow extends Workflow<{ v: number }, { done: true }> {
    readonly name = "timeout-lease-test";
    readonly firstStep = ShortStep;
    readonly steps = [ShortStep, DefaultTimeoutStep];
    readonly terminalSteps = [ShortStep, DefaultTimeoutStep];
}

const workflow = defineWorkflow(TimeoutLeaseWorkflow);

describe("timeout / lease helpers", () => {
    test("resolveStepTimeoutMs uses override or default", () => {
        expect(resolveStepTimeoutMs(undefined, 60_000)).toBe(60_000);
        expect(resolveStepTimeoutMs(2_000, 60_000)).toBe(2_000);
    });

    test("computeLeaseTtlMs adds buffer above step timeout", () => {
        expect(computeLeaseTtlMs(60_000, 30_000)).toBe(90_000);
        expect(computeLeaseTtlMs(5_000, 500)).toBe(5_500);
    });

    test("defaults are 60s timeout and 30s lease buffer", () => {
        expect(DEFAULT_STEP_TIMEOUT_MS).toBe(60_000);
        expect(DEFAULT_LEASE_BUFFER_MS).toBe(30_000);
        expect(computeLeaseTtlMs(DEFAULT_STEP_TIMEOUT_MS, DEFAULT_LEASE_BUFFER_MS)).toBe(90_000);
    });
});

describe("StepRegistry timeout / lease", () => {
    const baseRegistry = () =>
        new StepRegistry(
            [{ name: "default", concurrency: 1 }],
            { ShortStep: "default", DefaultTimeoutStep: "default" },
            [workflow],
        );

    test("step override drives timeout and derived lease", () => {
        const registry = baseRegistry();
        expect(registry.getEffectiveStepTimeoutMs("ShortStep")).toBe(5_000);
        expect(registry.getLeaseTtlMs("ShortStep")).toBe(5_000 + DEFAULT_LEASE_BUFFER_MS);
    });

    test("steps without override use global default timeout and lease", () => {
        const registry = baseRegistry();
        expect(registry.getEffectiveStepTimeoutMs("DefaultTimeoutStep")).toBe(
            DEFAULT_STEP_TIMEOUT_MS,
        );
        expect(registry.getLeaseTtlMs("DefaultTimeoutStep")).toBe(
            DEFAULT_STEP_TIMEOUT_MS + DEFAULT_LEASE_BUFFER_MS,
        );
    });

    test("custom defaultStepTimeoutMs and leaseBufferMs apply", () => {
        const registry = new StepRegistry(
            [{ name: "default", concurrency: 1 }],
            { ShortStep: "default", DefaultTimeoutStep: "default" },
            [workflow],
            { defaultStepTimeoutMs: 2_000, leaseBufferMs: 500 },
        );
        expect(registry.getEffectiveStepTimeoutMs("DefaultTimeoutStep")).toBe(2_000);
        expect(registry.getLeaseTtlMs("DefaultTimeoutStep")).toBe(2_500);
        expect(registry.getLeaseTtlMs("ShortStep")).toBe(5_500);
    });

    test("rejects invalid defaultStepTimeoutMs", () => {
        expect(
            () =>
                new StepRegistry(
                    [{ name: "default", concurrency: 1 }],
                    { ShortStep: "default", DefaultTimeoutStep: "default" },
                    [workflow],
                    { defaultStepTimeoutMs: 0 },
                ),
        ).toThrow("defaultStepTimeoutMs must be >= 1");
    });
});
