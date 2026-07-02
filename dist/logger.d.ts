export interface ChotuLogger {
    info(msg: string, ...args: unknown[]): void;
    warn(msg: string, ...args: unknown[]): void;
    error(msg: string, ...args: unknown[]): void;
}
export declare const defaultLogger: ChotuLogger;
//# sourceMappingURL=logger.d.ts.map