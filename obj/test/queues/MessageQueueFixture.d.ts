import { IMessageQueue } from 'pip-services3-messaging-nodex';
export declare class MessageQueueFixture {
    private _queue;
    constructor(queue: IMessageQueue);
    testSendReceiveMessage(): Promise<void>;
    testReceiveSendMessage(): Promise<void>;
    testReceiveCompleteMessage(): Promise<void>;
    testReceiveAbandonMessage(): Promise<void>;
    testSendPeekMessage(): Promise<void>;
    testPeekNoMessage(): Promise<void>;
    testMoveToDeadMessage(): Promise<void>;
    testOnMessage(): Promise<void>;
}
