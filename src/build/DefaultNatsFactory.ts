/** @module build */
import { Factory } from 'pip-services3-components-nodex';
import { Descriptor } from 'pip-services3-commons-nodex';

import { NatsMessageQueue } from '../queues/NatsMessageQueue';
import { NatsBareMessageQueue } from '../queues/NatsBareMessageQueue';
import { NatsConnection } from '../connect/NatsConnection';
import { NatsMessageQueueFactory } from './NatsMessageQueueFactory';

/**
 * Creates [[NatsMessageQueue]] components by their descriptors.
 * 
 * @see [[NatsMessageQueue]]
 */
export class DefaultNatsFactory extends Factory {
    private static readonly NatsQueueDescriptor: Descriptor = new Descriptor("pip-services", "message-queue", "nats", "*", "1.0");
    private static readonly NatsBareQueueDescriptor: Descriptor = new Descriptor("pip-services", "message-queue", "bare-nats", "*", "1.0");
	private static readonly NatsConnectionDescriptor: Descriptor = new Descriptor("pip-services", "connection", "nats", "*", "1.0");
	private static readonly NatsQueueFactoryDescriptor: Descriptor = new Descriptor("pip-services", "queue-factory", "nats", "*", "1.0");

	/**
	 * Create a new instance of the factory.
	 */
	public constructor() {
        super();
        this.register(DefaultNatsFactory.NatsQueueDescriptor, (locator: Descriptor) => {
            let name = (typeof locator.getName === "function") ? locator.getName() : null; 
            return new NatsMessageQueue(name);
        });
        this.register(DefaultNatsFactory.NatsBareQueueDescriptor, (locator: Descriptor) => {
            let name = (typeof locator.getName === "function") ? locator.getName() : null; 
            return new NatsBareMessageQueue(name);
        });
		this.registerAsType(DefaultNatsFactory.NatsConnectionDescriptor, NatsConnection);
		this.registerAsType(DefaultNatsFactory.NatsQueueFactoryDescriptor, NatsMessageQueueFactory);
	}
}