import { defaultLogger } from "./logger";
const WORKFLOW_RUN_STATUS = ["running", "completed", "failed", "cancelled"];
const STEP_STATUS = ["pending", "running", "completed", "failed", "waiting", "cancelled"];
const WORKFLOW_RUN_STATUS_CHECK = WORKFLOW_RUN_STATUS.map((s) => `'${s}'`).join(", ");
const STEP_STATUS_CHECK = STEP_STATUS.map((s) => `'${s}'`).join(", ");
async function migrationApplied(sql, version) {
    const [row] = await sql `
        SELECT 1 FROM chotu.schema_migrations WHERE version = ${version}
    `;
    return Boolean(row);
}
async function recordMigration(sql, version, name) {
    await sql `
        INSERT INTO chotu.schema_migrations (version, name)
        VALUES (${version}, ${name})
        ON CONFLICT (version) DO NOTHING
    `;
}
const migrations = [
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
        },
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
        },
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
        },
    },
    {
        version: 4,
        name: "fk_cascade",
        up: async (sql) => {
            const [constraint] = await sql `
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
                await sql.unsafe(`ALTER TABLE chotu.step_executions DROP CONSTRAINT "${constraint.name}"`);
            }
            await sql.unsafe(`
                ALTER TABLE chotu.step_executions
                    ADD CONSTRAINT step_executions_workflow_run_id_fkey
                    FOREIGN KEY (workflow_run_id)
                    REFERENCES chotu.workflow_runs(id)
                    ON DELETE CASCADE
            `);
        },
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
        },
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
        },
    },
    {
        version: 7,
        name: "drop_active_step_unique_index",
        up: async (sql) => {
            await sql.unsafe(`
                DROP INDEX IF EXISTS chotu.step_executions_active_step_per_run_idx
            `);
        },
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
        },
    },
    {
        version: 10,
        name: "cancelled_status",
        up: async (sql) => {
            const workflowConstraints = await sql `
                SELECT con.conname AS name
                FROM pg_constraint con
                JOIN pg_class rel ON rel.oid = con.conrelid
                JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
                WHERE nsp.nspname = 'chotu'
                    AND rel.relname = 'workflow_runs'
                    AND con.contype = 'c'
            `;
            for (const row of workflowConstraints) {
                await sql.unsafe(`ALTER TABLE chotu.workflow_runs DROP CONSTRAINT "${row.name}"`);
            }
            await sql.unsafe(`
                ALTER TABLE chotu.workflow_runs
                    ADD CONSTRAINT workflow_runs_status_check
                    CHECK (status IN (${WORKFLOW_RUN_STATUS_CHECK}))
            `);
            const stepConstraints = await sql `
                SELECT con.conname AS name
                FROM pg_constraint con
                JOIN pg_class rel ON rel.oid = con.conrelid
                JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
                WHERE nsp.nspname = 'chotu'
                    AND rel.relname = 'step_executions'
                    AND con.contype = 'c'
            `;
            for (const row of stepConstraints) {
                await sql.unsafe(`ALTER TABLE chotu.step_executions DROP CONSTRAINT "${row.name}"`);
            }
            await sql.unsafe(`
                ALTER TABLE chotu.step_executions
                    ADD CONSTRAINT step_executions_status_check
                    CHECK (status IN (${STEP_STATUS_CHECK}))
            `);
        },
    },
];
export async function ensureSchema(sql, logger = defaultLogger) {
    await sql `CREATE SCHEMA IF NOT EXISTS chotu`;
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
//# sourceMappingURL=schema.js.map