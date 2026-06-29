export async function writeWithPgFallback<T>(params: {
    redisWrite: () => Promise<T>;
    pgWrite: (result: T) => Promise<void>;
    rollback: (result: T) => Promise<void>;
}): Promise<T> {
    const result = await params.redisWrite();
    try {
        await params.pgWrite(result);
    } catch (err) {
        await params.rollback(result);
        throw err;
    }
    return result;
}
