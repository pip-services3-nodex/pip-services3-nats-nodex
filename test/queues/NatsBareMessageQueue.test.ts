let process = require('process');

import { ConfigParams } from 'pip-services3-commons-nodex';

import { MessageQueueFixture } from './MessageQueueFixture';
import { NatsBareMessageQueue } from '../../src/queues/NatsBareMessageQueue';

suite('NatsBareMessageQueue', ()=> {
    let queue: NatsBareMessageQueue;
    let fixture: MessageQueueFixture;

    let brokerHost = process.env['NATS_SERVICE_HOST'] || 'localhost';
    let brokerPort = process.env['NATS_SERVICE_PORT'] || 4222;
    if (brokerHost == '' && brokerPort == '') {
        return;
    }
    let brokerQueue = process.env['NATS_QUEUE'] || 'test';
    let brokerUser = process.env['NATS_USER'];
    let brokerPass = process.env['NATS_PASS'];
    let brokerToken = process.env['NATS_TOKEN'];

    let queueConfig = ConfigParams.fromTuples(
        'queue', brokerQueue,
        'connection.protocol', 'nats',
        'connection.host', brokerHost,
        'connection.port', brokerPort,
        'credential.username', brokerUser,
        'credential.password', brokerPass,
        'credential.token', brokerToken,
    );        

    setup(async () => {
        queue = new NatsBareMessageQueue(brokerQueue);
        queue.configure(queueConfig);

        fixture = new MessageQueueFixture(queue);

        await queue.open(null);
        // await queue.clear(null);
    });

    teardown(async () => {
        await queue.close(null);
    });

    test('Receive and Send Message', async () => {
       await fixture.testReceiveSendMessage();
    });

    test('On Message', async () => {
       await fixture.testOnMessage();
    });

});