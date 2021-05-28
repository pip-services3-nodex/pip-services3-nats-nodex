"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MessageQueueFixture = void 0;
const assert = require('chai').assert;
const pip_services3_messaging_nodex_1 = require("pip-services3-messaging-nodex");
const pip_services3_messaging_nodex_2 = require("pip-services3-messaging-nodex");
class MessageQueueFixture {
    constructor(queue) {
        this._queue = queue;
    }
    testSendReceiveMessage() {
        return __awaiter(this, void 0, void 0, function* () {
            let envelope1 = new pip_services3_messaging_nodex_1.MessageEnvelope("123", "Test", "Test message");
            yield this._queue.send(null, envelope1);
            let envelope2 = yield this._queue.receive(null, 10000);
            assert.isNotNull(envelope2);
            assert.equal(envelope1.message_type, envelope2.message_type);
            assert.equal(envelope1.message.toString(), envelope2.message.toString());
            assert.equal(envelope1.correlation_id, envelope2.correlation_id);
        });
    }
    testReceiveSendMessage() {
        return __awaiter(this, void 0, void 0, function* () {
            let envelope1 = new pip_services3_messaging_nodex_1.MessageEnvelope("123", "Test", "Test message");
            setTimeout(() => {
                this._queue.send(null, envelope1);
            }, 500);
            let envelope2 = yield this._queue.receive(null, 10000);
            assert.isNotNull(envelope2);
            assert.equal(envelope1.message_type, envelope2.message_type);
            assert.equal(envelope1.message.toString(), envelope2.message.toString());
            assert.equal(envelope1.correlation_id, envelope2.correlation_id);
        });
    }
    testReceiveCompleteMessage() {
        return __awaiter(this, void 0, void 0, function* () {
            let envelope1 = new pip_services3_messaging_nodex_1.MessageEnvelope("123", "Test", "Test message");
            yield this._queue.send(null, envelope1);
            let count = yield this._queue.readMessageCount();
            assert.isTrue(count > 0);
            let envelope2 = yield this._queue.receive(null, 10000);
            assert.isNotNull(envelope2);
            assert.equal(envelope1.message_type, envelope2.message_type);
            assert.equal(envelope1.message.toString(), envelope2.message.toString());
            assert.equal(envelope1.correlation_id, envelope2.correlation_id);
            yield this._queue.complete(envelope2);
            assert.isNull(envelope2.getReference());
        });
    }
    testReceiveAbandonMessage() {
        return __awaiter(this, void 0, void 0, function* () {
            let envelope1 = new pip_services3_messaging_nodex_1.MessageEnvelope("123", "Test", "Test message");
            yield this._queue.send(null, envelope1);
            let envelope2 = yield this._queue.receive(null, 10000);
            assert.isNotNull(envelope2);
            assert.equal(envelope1.message_type, envelope2.message_type);
            assert.equal(envelope1.message.toString(), envelope2.message.toString());
            assert.equal(envelope1.correlation_id, envelope2.correlation_id);
            yield this._queue.abandon(envelope2);
            envelope2 = yield this._queue.receive(null, 10000);
            assert.isNotNull(envelope2);
            assert.equal(envelope1.message_type, envelope2.message_type);
            assert.equal(envelope1.message.toString(), envelope2.message.toString());
            assert.equal(envelope1.correlation_id, envelope2.correlation_id);
        });
    }
    testSendPeekMessage() {
        return __awaiter(this, void 0, void 0, function* () {
            let envelope1 = new pip_services3_messaging_nodex_1.MessageEnvelope("123", "Test", "Test message");
            yield this._queue.send(null, envelope1);
            // Delay until the message is received
            yield new Promise((resolve, reject) => {
                setTimeout(resolve, 500);
            });
            let envelope2 = yield this._queue.peek(null);
            assert.isNotNull(envelope2);
            assert.equal(envelope1.message_type, envelope2.message_type);
            assert.equal(envelope1.message.toString(), envelope2.message.toString());
            assert.equal(envelope1.correlation_id, envelope2.correlation_id);
        });
    }
    testPeekNoMessage() {
        return __awaiter(this, void 0, void 0, function* () {
            let envelope = yield this._queue.peek(null);
            assert.isNull(envelope);
        });
    }
    testMoveToDeadMessage() {
        return __awaiter(this, void 0, void 0, function* () {
            let envelope1 = new pip_services3_messaging_nodex_1.MessageEnvelope("123", "Test", "Test message");
            yield this._queue.send(null, envelope1);
            let envelope2 = yield this._queue.receive(null, 10000);
            assert.isNotNull(envelope2);
            assert.equal(envelope1.message_type, envelope2.message_type);
            assert.equal(envelope1.message.toString(), envelope2.message.toString());
            assert.equal(envelope1.correlation_id, envelope2.correlation_id);
            yield this._queue.moveToDeadLetter(envelope2);
        });
    }
    testOnMessage() {
        return __awaiter(this, void 0, void 0, function* () {
            let messageReceiver = new pip_services3_messaging_nodex_2.TestMessageReceiver();
            this._queue.beginListen(null, messageReceiver);
            yield new Promise((resolve, reject) => setTimeout(resolve, 1000));
            let envelope1 = new pip_services3_messaging_nodex_1.MessageEnvelope("123", "Test", "Test message");
            yield this._queue.send(null, envelope1);
            yield new Promise((resolve, reject) => setTimeout(resolve, 1000));
            let envelope2 = messageReceiver.messages[0];
            assert.isNotNull(envelope2);
            assert.equal(envelope1.message_type, envelope2.message_type);
            assert.equal(envelope1.message.toString(), envelope2.message.toString());
            assert.equal(envelope1.correlation_id, envelope2.correlation_id);
            this._queue.endListen(null);
        });
    }
}
exports.MessageQueueFixture = MessageQueueFixture;
//# sourceMappingURL=MessageQueueFixture.js.map