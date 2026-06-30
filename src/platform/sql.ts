import postgresImport from "postgres";

export type ChotuSql = postgresImport.Sql<any>;

function loadPostgres(): typeof postgresImport {
    if (typeof postgresImport === "function") {
        return postgresImport;
    }
    const mod = postgresImport as { default: typeof postgresImport };
    return mod.default;
}

export function createSql(
    url: string,
    options: { max?: number } = {},
): ChotuSql {
    return loadPostgres()(url, {
        max: options.max ?? 10,
    });
}
