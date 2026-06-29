import type { SQL } from "bun";
import { fromPgRow } from "../../domain/execution.mapper";
import type { IWorkflowRepository } from "../../interfaces/repository.interface";
import {
    StepExecutionStatus,
    WorkflowRunStatus,
    type StepExecution,
    type WorkflowCompleteInput,
    type WorkflowRun,
} from "../../interfaces/workflow.interface";

export class PgRepository implements IWorkflowRepository {
    constructor(private readonly sql: SQL) {}

    async getWorkflowRun(id: string): Promise<WorkflowRun | null> {
        const [row] = await this.sql`
            SELECT *
            FROM chotu.workflow_runs
            WHERE id = ${id}
        `;
        if (!row) return null;
        return this.mapWorkflowRun(row);
    }

    async getStepExecutions(workflowRunId: string): Promise<StepExecution[]> {
        const rows = await this.sql`
            SELECT *
            FROM chotu.step_executions
            WHERE workflow_run_id = ${workflowRunId}
            ORDER BY created_at ASC
        `;
        return rows.map((row: Record<string, unknown>) => fromPgRow(row));
    }

    async insertWorkflowRunWithFirstStep(params: {
        workflowRunId: string;
        workflowName: string;
        input: Record<string, any>;
        firstStepId: string;
        firstStepName: string;
        queue: string;
    }): Promise<void> {
        const now = new Date();
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
                    ${WorkflowRunStatus.RUNNING},
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
                    ${StepExecutionStatus.PENDING},
                    ${params.input},
                    0,
                    ${now},
                    ${now}
                )
            `;
        });
    }

    async insertStep(params: {
        id: string;
        workflowRunId: string;
        stepName: string;
        queue: string;
        status: StepExecutionStatus;
        input: Record<string, any> | null;
        joinStepId?: string | null;
        fanOutIndex?: number | null;
        joinTotal?: number | null;
        joinRemaining?: number | null;
    }): Promise<void> {
        const now = new Date();
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

    async syncStepTerminal(params: {
        id: string;
        status: StepExecutionStatus.COMPLETED | StepExecutionStatus.FAILED;
        output?: Record<string, any> | null;
        error?: Record<string, any> | null;
        version: number;
    }): Promise<void> {
        const now = new Date();
        if (params.status === StepExecutionStatus.COMPLETED) {
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

    async syncJoinFinalize(params: {
        id: string;
        input: Record<string, any>[];
        version: number;
    }): Promise<void> {
        const now = new Date();
        await this.sql`
            UPDATE chotu.step_executions
            SET status = ${StepExecutionStatus.PENDING},
                input = ${params.input},
                join_remaining = NULL,
                join_total = NULL,
                updated_at = ${now},
                version = ${params.version}
            WHERE id = ${params.id}
        `;
    }

    async syncJoinRemaining(id: string, remaining: number): Promise<void> {
        await this.sql`
            UPDATE chotu.step_executions
            SET join_remaining = ${remaining}, updated_at = NOW()
            WHERE id = ${id}
        `;
    }

    async syncWorkflowTerminal(params: {
        id: string;
        status: WorkflowRunStatus.COMPLETED | WorkflowRunStatus.FAILED;
        output: Record<string, any> | null;
        version: number;
    }): Promise<boolean> {
        const now = new Date();
        const [row] = await this.sql`
            UPDATE chotu.workflow_runs
            SET status = ${params.status},
                output = ${params.output},
                updated_at = ${now},
                finished_at = ${now},
                version = ${params.version}
            WHERE id = ${params.id}
                AND status = ${WorkflowRunStatus.RUNNING}
            RETURNING id
        `;
        return Boolean(row);
    }

    async syncStepAttempts(params: {
        id: string;
        attempts: number;
        updatedAt: string;
        version: number;
    }): Promise<boolean> {
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

    async syncStepStatus(params: {
        id: string;
        status: StepExecutionStatus;
        updatedAt: string;
        attempts?: number;
        version: number;
    }): Promise<boolean> {
        if (params.attempts != null) {
            const [row] = await this.sql`
                UPDATE chotu.step_executions
                SET status = ${params.status},
                    attempts = ${params.attempts},
                    updated_at = ${new Date(params.updatedAt)},
                    version = ${params.version}
                WHERE id = ${params.id} AND version <= ${params.version}
                RETURNING id
            `;
            return Boolean(row);
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

    async listPendingSteps(): Promise<
        { id: string; workflow_run_id: string; queue: string }[]
    > {
        const rows = await this.sql`
            SELECT se.id, se.workflow_run_id, se.queue
            FROM chotu.step_executions se
            INNER JOIN chotu.workflow_runs wr ON wr.id = se.workflow_run_id
            WHERE se.status = ${StepExecutionStatus.PENDING}
                AND wr.status = ${WorkflowRunStatus.RUNNING}
        `;
        return rows as { id: string; workflow_run_id: string; queue: string }[];
    }

    async getRunRow(workflowRunId: string): Promise<Record<string, unknown> | null> {
        const [row] = await this.sql`
            SELECT workflow_name, input, status
            FROM chotu.workflow_runs
            WHERE id = ${workflowRunId}
        `;
        return row ?? null;
    }

    async getCompleteStepRow(
        workflowRunId: string,
        completeStepName: string,
    ): Promise<Record<string, unknown> | null> {
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

    async insertCompleteStep(params: {
        id: string;
        workflowRunId: string;
        stepName: string;
        queue: string;
        input: WorkflowCompleteInput;
    }): Promise<void> {
        const now = new Date();
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
                ${StepExecutionStatus.PENDING},
                ${params.input},
                0,
                ${now},
                ${now}
            )
        `;
    }

    async completeWorkflowFromCompleteStep(params: {
        workflowRunId: string;
        output: Record<string, any> | null;
        version?: number;
    }): Promise<boolean> {
        const now = new Date();
        const [row] = await this.sql`
            UPDATE chotu.workflow_runs
            SET status = ${WorkflowRunStatus.COMPLETED},
                output = ${params.output ?? null},
                updated_at = ${now},
                finished_at = ${now},
                version = COALESCE(${params.version ?? null}, version)
            WHERE id = ${params.workflowRunId}
                AND status = ${WorkflowRunStatus.RUNNING}
            RETURNING id
        `;
        return Boolean(row);
    }

    async getStepRow(id: string): Promise<Record<string, unknown> | null> {
        const [row] = await this.sql`
            SELECT *
            FROM chotu.step_executions
            WHERE id = ${id}
        `;
        return row ?? null;
    }

    async getRunForHydrate(id: string): Promise<Record<string, unknown> | null> {
        const [row] = await this.sql`
            SELECT *
            FROM chotu.workflow_runs
            WHERE id = ${id}
        `;
        return row ?? null;
    }

    async getTerminalStepOutputs(
        workflowRunId: string,
        terminalNames: string[],
    ): Promise<{ step_name: string; output: Record<string, any> | null }[]> {
        const rows = await this.sql`
            SELECT DISTINCT ON (step_name) step_name, output
            FROM chotu.step_executions
            WHERE workflow_run_id = ${workflowRunId}
                AND status = ${StepExecutionStatus.COMPLETED}
            ORDER BY step_name, finished_at DESC NULLS LAST
        `;
        const names = new Set(terminalNames);
        return (rows as { step_name: string; output: Record<string, any> | null }[]).filter(
            (r) => names.has(r.step_name),
        );
    }

    private mapWorkflowRun(row: Record<string, unknown>): WorkflowRun {
        return {
            id: row.id as string,
            workflowName: row.workflow_name as string,
            status: row.status as WorkflowRunStatus,
            input: row.input as Record<string, any>,
            output: row.output as Record<string, any> | null,
            createdAt: row.created_at as Date,
            updatedAt: row.updated_at as Date,
            finishedAt: row.finished_at as Date | null,
        };
    }
}
