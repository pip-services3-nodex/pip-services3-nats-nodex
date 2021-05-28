export interface INatsMessageListener {
    onMessage(err: any, message: any): void;
}
