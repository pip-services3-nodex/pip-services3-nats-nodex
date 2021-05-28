let process = require('process');

import { ConfigParams } from 'pip-services3-commons-nodex';

import { MessageQueueFixture } from './MessageQueueFixture';
import { NatsMessageQueue } from '../../src/queues/NatsMessageQueue';

suite('NatsMessageQueue', ()=> {
    let queue: NatsMessageQueue;
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
        'options.autosubscribe', true
    );        

    setup(async () => {
        queue = new NatsMessageQueue(brokerQueue);
        queue.configure(queueConfig);

        fixture = new MessageQueueFixture(queue);

        await queue.open(null);
        // queue.clear(null);
    });

    teardown(async () => {
        await queue.close(null);
    });

    test('Send and Receive Message', async () => {
        await fixture.testSendReceiveMessage();
     });
 
    test('Receive and Send Message', async () => {
       await fixture.testReceiveSendMessage();
    });

    test('Send Peek Message', async () => {
        await fixture.testSendPeekMessage();
    });

    test('Peek No Message', async () => {
        await fixture.testPeekNoMessage();
    });
      
    test('On Message', async () => {
        await fixture.testOnMessage();
    });

});