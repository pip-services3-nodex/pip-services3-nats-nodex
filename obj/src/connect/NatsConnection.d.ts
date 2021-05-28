import { IReferenceable } from 'pip-services3-commons-nodex';
import { IReferences } from 'pip-services3-commons-nodex';
import { IConfigurable } from 'pip-services3-commons-nodex';
import { IOpenable } from 'pip-services3-commons-nodex';
import { ConfigParams } from 'pip-services3-commons-nodex';
import { CompositeLogger } from 'pip-services3-components-nodex';
import { IMessageQueueConnection } from 'pip-services3-messaging-nodex';
import { NatsConnectionResolver } from './NatsConnectionResolver';
import { INatsMessageListener } from './INatsMessageListener';
import { NatsSubscription } from './NatsSubscription';
/**
 * NATS connection using plain driver.
 *
 * By defining a connection and sharing it through multiple message queues
 * you can reduce number of used database connections.
 *
 * ### Configuration parameters ###
 *
 * - client_id:               (optional) name of the client id
 * - connection(s):
 *   - discovery_key:             (optional) a key to retrieve the connection from [[https://pip-services3-nodex.github.io/pip-services3-components-nodex/interfaces/connect.idiscovery.html IDiscovery]]
 *   - host:                      host name or IP address
 *   - port:                      port number (default: 27017)
 *   - uri:                       resource URI or connection string with all parameters in it
 * - credential(s):
 *   - store_key:                 (optional) a key to retrieve the credentials from [[https://pip-services3-nodex.github.io/pip-services3-components-nodex/interfaces/auth.icredentialstore.html ICredentialStore]]
 *   - username:                  user name
 *   - password:                  user password
 * - options:
 *   - retry_connect:        (optional) turns on/off automated reconnect when connection is log (default: true)
 *   - max_reconnect:        (optional) maximum reconnection attempts (default: 3)
 *   - reconnect_timeout:    (optional) number of milliseconds to wait on each reconnection attempt (default: 3000)
 *   - flush_timeout:        (optional) number of milliseconds to wait on flushing messages (default: 3000)
 *
 * ### References ###
 *
 * - <code>\*:logger:\*:\*:1.0</code>           (optional) [[https://pip-services3-nodex.github.io/pip-services3-components-nodex/interfaces/log.ilogger.html ILogger]] components to pass log messages
 * - <code>\*:discovery:\*:\*:1.0</code>        (optional) [[https://pip-services3-nodex.github.io/pip-services3-components-nodex/interfaces/connect.idiscovery.html IDiscovery]] services
 * - <code>\*:credential-store:\*:\*:1.0</code> (optional) Credential stores to resolve credentials
 *
 */
export declare class NatsConnection implements IMessageQueueConnection, IReferenceable, IConfigurable, IOpenable {
    private _defaultConfig;
    /**
     * The logger.
     */
    protected _logger: CompositeLogger;
    /**
     * The connection resolver.
     */
    protected _connectionResolver: NatsConnectionResolver;
    /**
     * The configuration options.
     */
    protected _options: ConfigParams;
    /**
     * The NATS connection pool object.
     */
    protected _connection: any;
    /**
     * Topic subscriptions
     */
    protected _subscriptions: NatsSubscription[];
    protected _clientId: string;
    protected _retryConnect: boolean;
    protected _maxReconnect: number;
    protected _reconnectTimeout: number;
    protected _flushTimeout: number;
    /**
     * Creates a new instance of the connection component.
     */
    constructor();
    /**
     * Configures component by passing configuration parameters.
     *
     * @param config    configuration parameters to be set.
     */
    configure(config: ConfigParams): void;
    /**
     * Sets references to dependent components.
     *
     * @param references 	references to locate the component dependencies.
     */
    setReferences(references: IReferences): void;
    /**
     * Checks if the component is opened.
     *
     * @returns true if the component has been opened and false otherwise.
     */
    isOpen(): boolean;
    /**
     * Opens the component.
     *
     * @param correlationId 	(optional) transaction id to trace execution through call chain.
     */
    open(correlationId: string): Promise<void>;
    /**
     * Closes component and frees used resources.
     *
     * @param correlationId 	(optional) transaction id to trace execution through call chain.
     */
    close(correlationId: string): Promise<void>;
    getConnection(): any;
    /**
     * Reads a list of registered queue names.
     * If connection doesn't support this function returnes an empty list.
     * @returns a list with registered queue names.
     */
    readQueueNames(): Promise<string[]>;
    /**
     * Creates a message queue.
     * If connection doesn't support this function it exists without error.
     * @param name the name of the queue to be created.
     */
    createQueue(name: string): Promise<void>;
    /**
     * Deletes a message queue.
     * If connection doesn't support this function it exists without error.
     * @param name the name of the queue to be deleted.
     */
    deleteQueue(name: string): Promise<void>;
    /**
     * Checks if connection is open
     * @returns an error is connection is closed or <code>null<code> otherwise.
     */
    protected checkOpen(): void;
    /**
     * Publish a message to a specified topic
     * @param subject a subject(topic) where the message will be placed
     * @param message a message to be published
     */
    publish(subject: string, message: any): Promise<void>;
    /**
     * Subscribe to a topic
     * @param subject a subject(topic) name
     * @param options subscription options
     * @param listener a message listener
     */
    subscribe(subject: string, options: any, listener: INatsMessageListener): Promise<void>;
    /**
     * Unsubscribe from a previously subscribed topic
     * @param subject a subject(topic) name
     * @param listener a message listener
     */
    unsubscribe(subject: string, listener: INatsMessageListener): Promise<void>;
}
