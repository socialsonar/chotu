import postgres from "postgres";

export type ChotuSql = postgres.Sql<any>;

export function createSql(
    url: string,
    options: { max?: number } = {},
): ChotuSql {
    return postgres(url, {
        max: options.max ?? 10,
    });
}
