import postgresImport from "postgres";
export type ChotuSql = postgresImport.Sql<any>;
export declare function createSql(url: string, options?: {
    max?: number;
}): ChotuSql;
//# sourceMappingURL=sql.d.ts.map