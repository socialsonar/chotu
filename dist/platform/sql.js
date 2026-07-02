import postgres from "postgres";
export function createSql(url, options = {}) {
    return postgres(url, {
        max: options.max ?? 10,
    });
}
//# sourceMappingURL=sql.js.map