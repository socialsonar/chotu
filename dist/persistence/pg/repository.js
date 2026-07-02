import { fromPgRow } from "../../domain/execution.mapper";
import { StepExecutionStatus, WorkflowRunStatus, } from "../../interfaces/workflow.interface";
export class PgRepository {
    sql;
    constructor(sql) {
        this.sql = sql;
    }
    async getWorkflowRun(id) {
        const [row] = await this.sql `
            SELECT *
            FROM chotu.workflow_runs
            WHERE id = ${id}
        `;
        if (!row)
            return null;
        return this.mapWorkflowRun(row);
    }
    async getStepExecutions(workflowRunId) {
        const rows = await this.sql `
            SELECT *
            FROM chotu.step_executions
            WHERE workflow_run_id = ${workflowRunId}
            ORDER BY created_at ASC
        `;
        return rows.map((row) => fromPgRow(row));
    }
    async insertWorkflowRunWithFirstStep(params) {
        const now = new Date();
        await this.sql.begin(async (tx) => {
            await tx `
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
            await tx `
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
    async insertStep(params) {
        const now = new Date();
        await this.sql `
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
        const now = new Date();
        if (params.status === StepExecutionStatus.COMPLETED) {
            await this.sql `
                UPDATE chotu.step_executions
                SET status = ${params.status},
                    output = ${params.output ?? null},
                    error = NULL,
                    updated_at = ${now},
                    finished_at = ${now},
                    version = ${params.version}
                WHERE id = ${params.id}
            `;
        }
        else {
            await this.sql `
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
        const now = new Date();
        await this.sql `
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
    async syncJoinRemaining(id, remaining) {
        await this.sql `
            UPDATE chotu.step_executions
            SET join_remaining = ${remaining}, updated_at = NOW()
            WHERE id = ${id}
        `;
    }
    async syncWorkflowTerminal(params) {
        const now = new Date();
        const [row] = await this.sql `
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
    async syncStepAttempts(params) {
        const [row] = await this.sql `
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
            const [row] = await this.sql `
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
        const [row] = await this.sql `
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
        const rows = await this.sql `
            SELECT se.id, se.workflow_run_id, se.queue
            FROM chotu.step_executions se
            INNER JOIN chotu.workflow_runs wr ON wr.id = se.workflow_run_id
            WHERE se.status = ${StepExecutionStatus.PENDING}
                AND wr.status = ${WorkflowRunStatus.RUNNING}
        `;
        return rows;
    }
    async getRunRow(workflowRunId) {
        const [row] = await this.sql `
            SELECT workflow_name, input, status
            FROM chotu.workflow_runs
            WHERE id = ${workflowRunId}
        `;
        return row ?? null;
    }
    async getCompleteStepRow(workflowRunId, completeStepName) {
        const [row] = await this.sql `
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
        const now = new Date();
        await this.sql `
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
    async completeWorkflowFromCompleteStep(params) {
        const now = new Date();
        const [row] = await this.sql `
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
    async getStepRow(id) {
        const [row] = await this.sql `
            SELECT *
            FROM chotu.step_executions
            WHERE id = ${id}
        `;
        return row ?? null;
    }
    async getRunForHydrate(id) {
        const [row] = await this.sql `
            SELECT *
            FROM chotu.workflow_runs
            WHERE id = ${id}
        `;
        return row ?? null;
    }
    async getTerminalStepOutputs(workflowRunId, terminalNames) {
        const rows = await this.sql `
            SELECT DISTINCT ON (step_name) step_name, output
            FROM chotu.step_executions
            WHERE workflow_run_id = ${workflowRunId}
                AND status = ${StepExecutionStatus.COMPLETED}
            ORDER BY step_name, finished_at DESC NULLS LAST
        `;
        const names = new Set(terminalNames);
        return rows.filter((r) => names.has(r.step_name));
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
            finishedAt: row.finished_at,
        };
    }
}
//# sourceMappingURL=repository.js.map