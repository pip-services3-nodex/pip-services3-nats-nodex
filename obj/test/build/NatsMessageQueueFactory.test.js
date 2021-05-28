"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const assert = require('chai').assert;
const pip_services3_commons_nodex_1 = require("pip-services3-commons-nodex");
const NatsMessageQueueFactory_1 = require("../../src/build/NatsMessageQueueFactory");
suite('NatsMessageQueueFactory', () => {
    test('CreateMessageQueue', () => {
        let factory = new NatsMessageQueueFactory_1.NatsMessageQueueFactory();
        let descriptor = new pip_services3_commons_nodex_1.Descriptor("pip-services", "message-queue", "bare-nats", "test", "1.0");
        let canResult = factory.canCreate(descriptor);
        assert.isNotNull(canResult);
        let queue = factory.create(descriptor);
        assert.isNotNull(queue);
        assert.equal("test", queue.getName());
    });
});
//# sourceMappingURL=NatsMessageQueueFactory.test.js.map