export async function writeWithPgFallback(params) {
    const result = await params.redisWrite();
    try {
        await params.pgWrite(result);
    }
    catch (err) {
        await params.rollback(result);
        throw err;
    }
    return result;
}
//# sourceMappingURL=dual-write.js.map