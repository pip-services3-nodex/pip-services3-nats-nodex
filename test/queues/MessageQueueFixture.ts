const assert = require('chai').assert;

import { IMessageQueue } from 'pip-services3-messaging-nodex';
import { MessageEnvelope } from 'pip-services3-messaging-nodex';
import { TestMessageReceiver } from 'pip-services3-messaging-nodex';

export class MessageQueueFixture {
    private _queue: IMessageQueue;

    public constructor(queue: IMessageQueue) {
        this._queue = queue;
    }

    public async testSendReceiveMessage(): Promise<void> {
        let envelope1: MessageEnvelope = new MessageEnvelope("123", "Test", "Test message");
        await this._queue.send(null, envelope1);

        let envelope2 = await this._queue.receive(null, 10000);
        assert.isNotNull(envelope2);
        assert.equal(envelope1.message_type, envelope2.message_type);
        assert.equal(envelope1.message.toString(), envelope2.message.toString());
        assert.equal(envelope1.correlation_id, envelope2.correlation_id);
    }

    public async testReceiveSendMessage() {
        let envelope1: MessageEnvelope = new MessageEnvelope("123", "Test", "Test message");

        setTimeout(() => {
            this._queue.send(null, envelope1);
        }, 500);

        let envelope2 = await this._queue.receive(null, 10000);
        assert.isNotNull(envelope2);
        assert.equal(envelope1.message_type, envelope2.message_type);
        assert.equal(envelope1.message.toString(), envelope2.message.toString());
        assert.equal(envelope1.correlation_id, envelope2.correlation_id);
    }

    public async testReceiveCompleteMessage() {
        let envelope1: MessageEnvelope = new MessageEnvelope("123", "Test", "Test message");

        await this._queue.send(null, envelope1);

        let count = await this._queue.readMessageCount();
        assert.isTrue(count > 0);

        let envelope2 = await this._queue.receive(null, 10000);
        assert.isNotNull(envelope2);
        assert.equal(envelope1.message_type, envelope2.message_type);
        assert.equal(envelope1.message.toString(), envelope2.message.toString());
        assert.equal(envelope1.correlation_id, envelope2.correlation_id);

        await this._queue.complete(envelope2);
        assert.isNull(envelope2.getReference());
    }

    public async testReceiveAbandonMessage() {
        let envelope1: MessageEnvelope = new MessageEnvelope("123", "Test", "Test message");
        await this._queue.send(null, envelope1);

        let envelope2 = await this._queue.receive(null, 10000);
        assert.isNotNull(envelope2);
        assert.equal(envelope1.message_type, envelope2.message_type);
        assert.equal(envelope1.message.toString(), envelope2.message.toString());
        assert.equal(envelope1.correlation_id, envelope2.correlation_id);

        await this._queue.abandon(envelope2);

        envelope2 = await this._queue.receive(null, 10000);
        assert.isNotNull(envelope2);
        assert.equal(envelope1.message_type, envelope2.message_type);
        assert.equal(envelope1.message.toString(), envelope2.message.toString());
        assert.equal(envelope1.correlation_id, envelope2.correlation_id);
    }

    public async testSendPeekMessage() {
        let envelope1: MessageEnvelope = new MessageEnvelope("123", "Test", "Test message");
        await this._queue.send(null, envelope1);

        // Delay until the message is received
        await new Promise<void>((resolve, reject) => {
            setTimeout(resolve, 500);
        });

        let envelope2 = await this._queue.peek(null);
        assert.isNotNull(envelope2);
        assert.equal(envelope1.message_type, envelope2.message_type);
        assert.equal(envelope1.message.toString(), envelope2.message.toString());
        assert.equal(envelope1.correlation_id, envelope2.correlation_id);
    }

    public async testPeekNoMessage() {
        let envelope = await this._queue.peek(null);
        assert.isNull(envelope);
    }

    public async testMoveToDeadMessage() {
        let envelope1: MessageEnvelope = new MessageEnvelope("123", "Test", "Test message");
        await this._queue.send(null, envelope1);

        let envelope2 = await this._queue.receive(null, 10000);
        assert.isNotNull(envelope2);
        assert.equal(envelope1.message_type, envelope2.message_type);
        assert.equal(envelope1.message.toString(), envelope2.message.toString());
        assert.equal(envelope1.correlation_id, envelope2.correlation_id);

        await this._queue.moveToDeadLetter(envelope2);
    }

    public async testOnMessage() {
        let messageReceiver = new TestMessageReceiver();
        this._queue.beginListen(null, messageReceiver);

        await new Promise<void>((resolve, reject) => setTimeout(resolve, 1000));

        let envelope1: MessageEnvelope = new MessageEnvelope("123", "Test", "Test message");
        await this._queue.send(null, envelope1);

        await new Promise<void>((resolve, reject) => setTimeout(resolve, 1000));

        let envelope2 = messageReceiver.messages[0];
        assert.isNotNull(envelope2);
        assert.equal(envelope1.message_type, envelope2.message_type);
        assert.equal(envelope1.message.toString(), envelope2.message.toString());
        assert.equal(envelope1.correlation_id, envelope2.correlation_id);

        this._queue.endListen(null);
    }

}
