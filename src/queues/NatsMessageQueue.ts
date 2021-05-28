/** @module queues */
import { ConfigParams } from 'pip-services3-commons-nodex';
import { IMessageReceiver } from 'pip-services3-messaging-nodex';
import { MessageEnvelope } from 'pip-services3-messaging-nodex';
import { MessagingCapabilities } from 'pip-services3-messaging-nodex';

import { NatsAbstractMessageQueue } from './NatsAbstractMessageQueue';

/**
 * Message queue that sends and receives messages via NATS message broker.
 *  
 * ### Configuration parameters ###
 * 
 * - subject:                       name of NATS topic (subject) to subscribe
 * - queue_group:                   name of NATS queue group
 * - connection(s):
 *   - discovery_key:               (optional) a key to retrieve the connection from [[https://pip-services3-nodex.github.io/pip-services3-components-nodex/interfaces/connect.idiscovery.html IDiscovery]]
 *   - host:                        host name or IP address
 *   - port:                        port number
 *   - uri:                         resource URI or connection string with all parameters in it
 * - credential(s):
 *   - store_key:                   (optional) a key to retrieve the credentials from [[https://pip-services3-nodex.github.io/pip-services3-components-nodex/interfaces/auth.icredentialstore.html ICredentialStore]]
 *   - username:                    user name
 *   - password:                    user password
 * - options:
 *   - serialize_message:    (optional) true to serialize entire message as JSON, false to send only message payload (default: true)
 *   - autosubscribe:        (optional) true to automatically subscribe on option (default: false)
 *   - retry_connect:        (optional) turns on/off automated reconnect when connection is log (default: true)
 *   - max_reconnect:        (optional) maximum reconnection attempts (default: 3)
 *   - reconnect_timeout:    (optional) number of milliseconds to wait on each reconnection attempt (default: 3000)
 *   - flush_timeout:        (optional) number of milliseconds to wait on flushing messages (default: 3000)
 * 
 * ### References ###
 * 
 * - <code>\*:logger:\*:\*:1.0</code>             (optional) [[https://pip-services3-nodex.github.io/pip-services3-components-nodex/interfaces/log.ilogger.html ILogger]] components to pass log messages
 * - <code>\*:counters:\*:\*:1.0</code>           (optional) [[https://pip-services3-nodex.github.io/pip-services3-components-nodex/interfaces/count.icounters.html ICounters]] components to pass collected measurements
 * - <code>\*:discovery:\*:\*:1.0</code>          (optional) [[https://pip-services3-nodex.github.io/pip-services3-components-nodex/interfaces/connect.idiscovery.html IDiscovery]] services to resolve connections
 * - <code>\*:credential-store:\*:\*:1.0</code>   (optional) Credential stores to resolve credentials
 * - <code>\*:connection:nats:\*:1.0</code>       (optional) Shared connection to NATS service
 * 
 * @see [[MessageQueue]]
 * @see [[MessagingCapabilities]]
 * 
 * ### Example ###
 * 
 *     let queue = new NatsMessageQueue("myqueue");
 *     queue.configure(ConfigParams.fromTuples(
 *       "topic", "mytopic",
 *       "connection.protocol", "nats"
 *       "connection.host", "localhost"
 *       "connection.port", 1883
 *     ));
 * 
 *     queue.open("123", (err) => {
 *         ...
 *     });
 * 
 *     queue.send("123", new MessageEnvelope(null, "mymessage", "ABC"));
 * 
 *     queue.receive("123", (err, message) => {
 *         if (message != null) {
 *            ...
 *            queue.complete("123", message);
 *         }
 *     });
 */
export class NatsMessageQueue extends NatsAbstractMessageQueue {
    protected _autoSubscribe: boolean;
    protected _subscribed: boolean;
    protected _messages: MessageEnvelope[] = [];
    protected _receiver: IMessageReceiver;

    /**
     * Creates a new instance of the message queue.
     * 
     * @param name  (optional) a queue name.
     */
    public constructor(name?: string) {
        super(name, new MessagingCapabilities(false, true, true, true, true, false, false, false, true));
    }

    /**
     * Configures component by passing configuration parameters.
     * 
     * @param config    configuration parameters to be set.
     */
     public configure(config: ConfigParams): void {
         super.configure(config);

        this._autoSubscribe = config.getAsBooleanWithDefault("options.autosubscribe", this._autoSubscribe);
    }

    /**
     * Opens the component.
     * 
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     */
    public async open(correlationId: string): Promise<void> {
        if (this.isOpen()) {
            return;
        }

        try {
            await super.open(correlationId);

            // Subscribe right away
            if (this._autoSubscribe) {
                await this.subscribe(correlationId);
            }
        } catch (ex) {
            await this.close(correlationId);
            throw ex;
        }
    }

    /**
	 * Closes component and frees used resources.
	 * 
	 * @param correlationId 	(optional) transaction id to trace execution through call chain.
     */
    public async close(correlationId: string): Promise<void> {
        if (!this.isOpen()) {
            return;
        }

        // Unsubscribe from the topic
        if (this._subscribed) {
            let subject = this.getSubject();
            await this._connection.unsubscribe(subject, this);
            this._subscribed = false;
        }
    
        await super.close(correlationId);

        this._messages = [];
        this._receiver = null;
    }

    /**
	 * Clears component state.
	 * 
	 * @param correlationId 	(optional) transaction id to trace execution through call chain.
     */
    public async clear(correlationId: string): Promise<void> {
        this._messages = [];
    }

    protected async subscribe(correlationId: string): Promise<void> {
        if (this._subscribed) {
            return;
        }

        // Subscribe right away
        let subject = this.getSubject();
        await this._connection.subscribe(
            subject,
            { group: this._queueGroup },
            this
        );

        this._subscribed = true;
    }

    /**
     * Reads the current number of messages in the queue to be delivered.
     * 
     * @returns a number of messages in the queue.
     */
     public async readMessageCount(): Promise<number> {
        return this._messages.length;
    }

    /**
     * Peeks a single incoming message from the queue without removing it.
     * If there are no messages available in the queue it returns null.
     * 
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     * @returns a peeked message.
     */
    public async peek(correlationId: string): Promise<MessageEnvelope> {
        this.checkOpen(correlationId);

        // Subscribe to topic if needed
        await this.subscribe(correlationId);

        // Peek a message from the top
        let message: MessageEnvelope = null;
        if (this._messages.length > 0) {
            message = this._messages[0];
        }

        if (message != null) {
            this._logger.trace(message.correlation_id, "Peeked message %s on %s", message, this.getName());
        }
    
        return message;
    }

    /**
     * Peeks multiple incoming messages from the queue without removing them.
     * If there are no messages available in the queue it returns an empty list.
     * 
     * Important: This method is not supported by NATS.
     * 
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     * @param messageCount      a maximum number of messages to peek.
     * @returns a list with peeked messages.
     */
    public async peekBatch(correlationId: string, messageCount: number): Promise<MessageEnvelope[]> {
        this.checkOpen(correlationId);

        // Subscribe to topic if needed
        await this.subscribe(correlationId);

        // Peek a batch of messages
        let messages = this._messages.slice(0, messageCount);

        this._logger.trace(correlationId, "Peeked %d messages on %s", messages.length, this.getName());

        return messages;
    }

    /**
     * Receives an incoming message and removes it from the queue.
     * 
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     * @param waitTimeout       a timeout in milliseconds to wait for a message to come.
     * @returns a received message or <code>null</code> if nothing was received.
     */
    public async receive(correlationId: string, waitTimeout: number): Promise<MessageEnvelope> {
        this.checkOpen(correlationId);

        // Subscribe to topic if needed
        this.subscribe(correlationId);

        let message: MessageEnvelope = null;

        // Return message immediately if it exist
        if (this._messages.length > 0) {
            message = this._messages.shift();
            return message;
        }

        // Otherwise wait and return
        let checkInterval = 100;
        let elapsedTime = 0;
        while (true) {
            let test = this.isOpen() && elapsedTime < waitTimeout && message == null;
            if (!test) break;
            
            message = await new Promise<MessageEnvelope>((resolve, reject) => {
                setTimeout(() => {
                    let message = this._messages.shift();
                    resolve(message);
                }, checkInterval);
            });

            elapsedTime += checkInterval;
        }

        return message;
    }

    public onMessage(err: any, msg: any): void {
        if (err != null || msg == null) {
            this._logger.error(null, err, "Failed to receive a message");
            return;
        } 

        // Deserialize message
        let message = this.toMessage(msg)
        if (message == null) {
            this._logger.error(null, null, "Failed to read received message");
            return;
        }

        this._counters.incrementOne("queue." + this.getName() + ".received_messages");
        this._logger.debug(message.correlation_id, "Received message %s via %s", message, this.getName());

        // Send message to receiver if its set or put it into the queue
        if (this._receiver != null) {
            this.sendMessageToReceiver(this._receiver, message);
        } else {
            this._messages.push(message);
        }
    }

    private sendMessageToReceiver(receiver: IMessageReceiver, message: MessageEnvelope): void {
        let correlationId = message != null ? message.correlation_id : null;
        if (message == null || receiver == null) {
            this._logger.warn(correlationId, "NATS message was skipped.");
            return;
        }

        this._receiver.receiveMessage(message, this)
        .catch((err) => {
            this._logger.error(correlationId, err, "Failed to process the message");
        });
    }

    /**
     * Listens for incoming messages and blocks the current thread until queue is closed.
     * 
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     * @param receiver          a receiver to receive incoming messages.
     * 
     * @see [[IMessageReceiver]]
     * @see [[receive]]
     */
    public listen(correlationId: string, receiver: IMessageReceiver): void {
        this.checkOpen(correlationId);

        // Subscribe to topic if needed
        this.subscribe(correlationId)
        .then(() => {
            this._logger.trace(null, "Started listening messages at %s", this.getName());

            // Resend collected messages to receiver
            while (this.isOpen() && this._messages.length > 0) {
                let message = this._messages.shift();
                if (message != null) {
                    this.sendMessageToReceiver(receiver, message);
                }
            }

            // Set the receiver
            if (this.isOpen()) {
                this._receiver = receiver;
            }
        });
    }

    /**
     * Ends listening for incoming messages.
     * When this method is call [[listen]] unblocks the thread and execution continues.
     * 
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     */
    public endListen(correlationId: string): void {
        this._receiver = null;
    }

}