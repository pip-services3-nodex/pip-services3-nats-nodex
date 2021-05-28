"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DefaultNatsFactory = void 0;
/** @module build */
const pip_services3_components_nodex_1 = require("pip-services3-components-nodex");
const pip_services3_commons_nodex_1 = require("pip-services3-commons-nodex");
const NatsMessageQueue_1 = require("../queues/NatsMessageQueue");
const NatsBareMessageQueue_1 = require("../queues/NatsBareMessageQueue");
const NatsConnection_1 = require("../connect/NatsConnection");
const NatsMessageQueueFactory_1 = require("./NatsMessageQueueFactory");
/**
 * Creates [[NatsMessageQueue]] components by their descriptors.
 *
 * @see [[NatsMessageQueue]]
 */
class DefaultNatsFactory extends pip_services3_components_nodex_1.Factory {
    /**
     * Create a new instance of the factory.
     */
    constructor() {
        super();
        this.register(DefaultNatsFactory.NatsQueueDescriptor, (locator) => {
            let name = (typeof locator.getName === "function") ? locator.getName() : null;
            return new NatsMessageQueue_1.NatsMessageQueue(name);
        });
        this.register(DefaultNatsFactory.NatsBareQueueDescriptor, (locator) => {
            let name = (typeof locator.getName === "function") ? locator.getName() : null;
            return new NatsBareMessageQueue_1.NatsBareMessageQueue(name);
        });
        this.registerAsType(DefaultNatsFactory.NatsConnectionDescriptor, NatsConnection_1.NatsConnection);
        this.registerAsType(DefaultNatsFactory.NatsQueueFactoryDescriptor, NatsMessageQueueFactory_1.NatsMessageQueueFactory);
    }
}
exports.DefaultNatsFactory = DefaultNatsFactory;
DefaultNatsFactory.NatsQueueDescriptor = new pip_services3_commons_nodex_1.Descriptor("pip-services", "message-queue", "nats", "*", "1.0");
DefaultNatsFactory.NatsBareQueueDescriptor = new pip_services3_commons_nodex_1.Descriptor("pip-services", "message-queue", "bare-nats", "*", "1.0");
DefaultNatsFactory.NatsConnectionDescriptor = new pip_services3_commons_nodex_1.Descriptor("pip-services", "connection", "nats", "*", "1.0");
DefaultNatsFactory.NatsQueueFactoryDescriptor = new pip_services3_commons_nodex_1.Descriptor("pip-services", "queue-factory", "nats", "*", "1.0");
//# sourceMappingURL=DefaultNatsFactory.js.map