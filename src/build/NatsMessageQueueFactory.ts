/** @module build */
import { Descriptor } from 'pip-services3-commons-nodex';
import { IMessageQueue } from 'pip-services3-messaging-nodex';
import { MessageQueueFactory } from 'pip-services3-messaging-nodex';

import { NatsMessageQueue } from '../queues/NatsMessageQueue';
import { NatsBareMessageQueue } from '../queues/NatsBareMessageQueue';

/**
 * Creates [[NatsMessageQueue]] components by their descriptors.
 * Name of created message queue is taken from its descriptor.
 * 
 * @see [[https://pip-services3-nodex.github.io/pip-services3-components-nodex/classes/build.factory.html Factory]]
 * @see [[NatsMessageQueue]]
 */
export class NatsMessageQueueFactory extends MessageQueueFactory {
    private static readonly NatsQueueDescriptor: Descriptor = new Descriptor("pip-services", "message-queue", "nats", "*", "1.0");
    private static readonly NatsBareQueueDescriptor: Descriptor = new Descriptor("pip-services", "message-queue", "bare-nats", "*", "1.0");

    /**
	 * Create a new instance of the factory.
	 */
    public constructor() {
        super();
        this.register(NatsMessageQueueFactory.NatsQueueDescriptor, (locator: Descriptor) => {
            let name = (typeof locator.getName === "function") ? locator.getName() : null; 
            return this.createQueue(name);
        });
        this.register(NatsMessageQueueFactory.NatsBareQueueDescriptor, (locator: Descriptor) => {
            let name = (typeof locator.getName === "function") ? locator.getName() : null; 
            return this.createBareQueue(name);
        });
    }

    /**
     * Creates a message queue component and assigns its name.
     * @param name a name of the created message queue.
     */
     public createQueue(name: string): IMessageQueue {
        let queue = new NatsMessageQueue(name);

        if (this._config != null) {
            queue.configure(this._config);
        }
        if (this._references != null) {
            queue.setReferences(this._references);
        }

        return queue;        
    }

    /**
     * Creates a bare message queue component and assigns its name.
     * @param name a name of the created message queue.
     */
     public createBareQueue(name: string): IMessageQueue {
        let queue = new NatsBareMessageQueue(name);

        if (this._config != null) {
            queue.configure(this._config);
        }
        if (this._references != null) {
            queue.setReferences(this._references);
        }

        return queue;        
    }

}
