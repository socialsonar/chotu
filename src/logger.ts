export interface ChotuLogger {
    info(msg: string, ...args: unknown[]): void;
    warn(msg: string, ...args: unknown[]): void;
    error(msg: string, ...args: unknown[]): void;
}

export const defaultLogger: ChotuLogger = {
    info: (msg, ...args) => console.log(msg, ...args),
    warn: (msg, ...args) => console.warn(msg, ...args),
    error: (msg, ...args) => console.error(msg, ...args),
};
