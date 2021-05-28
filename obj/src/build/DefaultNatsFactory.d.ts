/** @module build */
import { Factory } from 'pip-services3-components-nodex';
/**
 * Creates [[NatsMessageQueue]] components by their descriptors.
 *
 * @see [[NatsMessageQueue]]
 */
export declare class DefaultNatsFactory extends Factory {
    private static readonly NatsQueueDescriptor;
    private static readonly NatsBareQueueDescriptor;
    private static readonly NatsConnectionDescriptor;
    private static readonly NatsQueueFactoryDescriptor;
    /**
     * Create a new instance of the factory.
     */
    constructor();
}
