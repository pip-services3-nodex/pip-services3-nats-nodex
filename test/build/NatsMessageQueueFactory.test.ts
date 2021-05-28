const assert = require('chai').assert;

import { Descriptor } from 'pip-services3-commons-nodex';

import { NatsMessageQueueFactory } from '../../src/build/NatsMessageQueueFactory';

suite('NatsMessageQueueFactory', ()=> {

    test('CreateMessageQueue', () => {
        let factory = new NatsMessageQueueFactory();
        let descriptor = new Descriptor("pip-services", "message-queue", "bare-nats", "test", "1.0");
    
        let canResult = factory.canCreate(descriptor);
        assert.isNotNull(canResult);
    
        let queue = factory.create(descriptor);
        assert.isNotNull(queue);
        assert.equal("test", queue.getName());
    });

});