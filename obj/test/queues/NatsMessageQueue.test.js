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
let process = require('process');
const pip_services3_commons_nodex_1 = require("pip-services3-commons-nodex");
const MessageQueueFixture_1 = require("./MessageQueueFixture");
const NatsMessageQueue_1 = require("../../src/queues/NatsMessageQueue");
suite('NatsMessageQueue', () => {
    let queue;
    let fixture;
    let brokerHost = process.env['NATS_SERVICE_HOST'] || 'localhost';
    let brokerPort = process.env['NATS_SERVICE_PORT'] || 4222;
    if (brokerHost == '' && brokerPort == '') {
        return;
    }
    let brokerQueue = process.env['NATS_QUEUE'] || 'test';
    let brokerUser = process.env['NATS_USER'];
    let brokerPass = process.env['NATS_PASS'];
    let brokerToken = process.env['NATS_TOKEN'];
    let queueConfig = pip_services3_commons_nodex_1.ConfigParams.fromTuples('queue', brokerQueue, 'connection.protocol', 'nats', 'connection.host', brokerHost, 'connection.port', brokerPort, 'credential.username', brokerUser, 'credential.password', brokerPass, 'credential.token', brokerToken, 'options.autosubscribe', true);
    setup(() => __awaiter(void 0, void 0, void 0, function* () {
        queue = new NatsMessageQueue_1.NatsMessageQueue(brokerQueue);
        queue.configure(queueConfig);
        fixture = new MessageQueueFixture_1.MessageQueueFixture(queue);
        yield queue.open(null);
        // queue.clear(null);
    }));
    teardown(() => __awaiter(void 0, void 0, void 0, function* () {
        yield queue.close(null);
    }));
    test('Send and Receive Message', () => __awaiter(void 0, void 0, void 0, function* () {
        yield fixture.testSendReceiveMessage();
    }));
    test('Receive and Send Message', () => __awaiter(void 0, void 0, void 0, function* () {
        yield fixture.testReceiveSendMessage();
    }));
    test('Send Peek Message', () => __awaiter(void 0, void 0, void 0, function* () {
        yield fixture.testSendPeekMessage();
    }));
    test('Peek No Message', () => __awaiter(void 0, void 0, void 0, function* () {
        yield fixture.testPeekNoMessage();
    }));
    test('On Message', () => __awaiter(void 0, void 0, void 0, function* () {
        yield fixture.testOnMessage();
    }));
});
//# sourceMappingURL=NatsMessageQueue.test.js.map