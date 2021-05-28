"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.NatsAbstractMessageQueue = void 0;
/** @module queues */
/** @hidden */
const nats = require('nats');
const pip_services3_commons_nodex_1 = require("pip-services3-commons-nodex");
const pip_services3_commons_nodex_2 = require("pip-services3-commons-nodex");
const pip_services3_commons_nodex_3 = require("pip-services3-commons-nodex");
const pip_services3_commons_nodex_4 = require("pip-services3-commons-nodex");
const pip_services3_commons_nodex_5 = require("pip-services3-commons-nodex");
const pip_services3_commons_nodex_6 = require("pip-services3-commons-nodex");
const pip_services3_components_nodex_1 = require("pip-services3-components-nodex");
const pip_services3_messaging_nodex_1 = require("pip-services3-messaging-nodex");
const pip_services3_messaging_nodex_2 = require("pip-services3-messaging-nodex");
const NatsConnection_1 = require("../connect/NatsConnection");
/**
 * Abstract NATS message queue with ability to connect to NATS server.
 */
class NatsAbstractMessageQueue extends pip_services3_messaging_nodex_1.MessageQueue {
    /**
     * Creates a new instance of the persistence component.
     *
     * @param name    (optional) a queue name.
     */
    constructor(name, capabilities) {
        super(name, capabilities);
        /**
         * The dependency resolver.
         */
        this._dependencyResolver = new pip_services3_commons_nodex_6.DependencyResolver(NatsAbstractMessageQueue._defaultConfig);
        /**
         * The logger.
         */
        this._logger = new pip_services3_components_nodex_1.CompositeLogger();
    }
    /**
     * Configures component by passing configuration parameters.
     *
     * @param config    configuration parameters to be set.
     */
    configure(config) {
        config = config.setDefaults(NatsAbstractMessageQueue._defaultConfig);
        this._config = config;
        this._dependencyResolver.configure(config);
        // this._serializeEnvelop = config.getAsBooleanWithDefault("options.serialize_envelop", this._serializeEnvelop)
        this._subject = config.getAsStringWithDefault("topic", this._subject);
        this._subject = config.getAsStringWithDefault("subject", this._subject);
        this._queueGroup = config.getAsStringWithDefault("group", this._queueGroup);
        this._queueGroup = config.getAsStringWithDefault("queue_group", this._queueGroup);
    }
    /**
     * Sets references to dependent components.
     *
     * @param references 	references to locate the component dependencies.
     */
    setReferences(references) {
        this._references = references;
        this._logger.setReferences(references);
        // Get connection
        this._dependencyResolver.setReferences(references);
        this._connection = this._dependencyResolver.getOneOptional('connection');
        // Or create a local one
        if (this._connection == null) {
            this._connection = this.createConnection();
            this._localConnection = true;
        }
        else {
            this._localConnection = false;
        }
    }
    /**
     * Unsets (clears) previously set references to dependent components.
     */
    unsetReferences() {
        this._connection = null;
    }
    createConnection() {
        let connection = new NatsConnection_1.NatsConnection();
        if (this._config)
            connection.configure(this._config);
        if (this._references)
            connection.setReferences(this._references);
        return connection;
    }
    /**
     * Checks if the component is opened.
     *
     * @returns true if the component has been opened and false otherwise.
     */
    isOpen() {
        return this._opened;
    }
    /**
     * Opens the component.
     *
     * @param correlationId 	(optional) transaction id to trace execution through call chain.
     */
    open(correlationId) {
        return __awaiter(this, void 0, void 0, function* () {
            if (this._opened) {
                return;
            }
            if (this._connection == null) {
                this._connection = this.createConnection();
                this._localConnection = true;
            }
            if (this._localConnection) {
                yield this._connection.open(correlationId);
            }
            if (!this._connection.isOpen()) {
                throw new pip_services3_commons_nodex_4.ConnectionException(correlationId, "CONNECT_FAILED", "NATS connection is not opened");
            }
            this._opened = true;
            this._client = this._connection.getConnection();
        });
    }
    /**
     * Closes component and frees used resources.
     *
     * @param correlationId 	(optional) transaction id to trace execution through call chain.
     */
    close(correlationId) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this._opened) {
                return;
            }
            if (this._connection == null) {
                throw new pip_services3_commons_nodex_5.InvalidStateException(correlationId, 'NO_CONNECTION', 'NATS connection is missing');
            }
            if (this._localConnection) {
                yield this._connection.close(correlationId);
            }
            this._opened = false;
            this._client = null;
        });
    }
    getSubject() {
        return this._subject != null && this._subject != "" ? this._subject : this._name;
    }
    fromMessage(message) {
        if (message == null)
            return null;
        let data = message.message || nats.Empty;
        let headers = nats.headers();
        headers.append("message_id", message.message_id);
        headers.append("correlation_id", message.correlation_id);
        headers.append("message_type", message.message_type);
        headers.append("sent_time", pip_services3_commons_nodex_2.StringConverter.toNullableString(message.sent_time || new Date()));
        return {
            data: data,
            headers: headers
        };
    }
    toMessage(msg) {
        if (msg == null)
            return null;
        let correlationId = msg.headers.get("correlation_id");
        let messageType = msg.headers.get("message_type");
        let message = new pip_services3_messaging_nodex_2.MessageEnvelope(correlationId, messageType, Buffer.from(msg.data));
        message.message_id = msg.headers.get("message_id");
        message.sent_time = pip_services3_commons_nodex_1.DateTimeConverter.toNullableDateTime(msg.headers.get("sent_time"));
        message.message = msg.data;
        return message;
    }
    /**
     * Clears component state.
     *
     * @param correlationId 	(optional) transaction id to trace execution through call chain.
     */
    clear(correlationId) {
        return __awaiter(this, void 0, void 0, function* () {
            // Not supported
        });
    }
    /**
     * Reads the current number of messages in the queue to be delivered.
     *
     * @returns number of messages in the queue.
     */
    readMessageCount() {
        return __awaiter(this, void 0, void 0, function* () {
            // Not supported
            return 0;
        });
    }
    /**
     * Sends a message into the queue.
     *
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     * @param message           a message envelop to be sent.
     */
    send(correlationId, message) {
        return __awaiter(this, void 0, void 0, function* () {
            this.checkOpen(correlationId);
            let subject = this.getName() || this._subject;
            let msg = this.fromMessage(message);
            yield this._connection.publish(subject, msg);
        });
    }
    /**
     * Renews a lock on a message that makes it invisible from other receivers in the queue.
     * This method is usually used to extend the message processing time.
     *
     * Important: This method is not supported by NATS.
     *
     * @param message       a message to extend its lock.
     * @param lockTimeout   a locking timeout in milliseconds.
     */
    renewLock(message, lockTimeout) {
        return __awaiter(this, void 0, void 0, function* () {
            // Not supported
        });
    }
    /**
     * Permanently removes a message from the queue.
     * This method is usually used to remove the message after successful processing.
     *
     * Important: This method is not supported by NATS.
     *
     * @param message   a message to remove.
     */
    complete(message) {
        return __awaiter(this, void 0, void 0, function* () {
            // Not supported
        });
    }
    /**
     * Returnes message into the queue and makes it available for all subscribers to receive it again.
     * This method is usually used to return a message which could not be processed at the moment
     * to repeat the attempt. Messages that cause unrecoverable errors shall be removed permanently
     * or/and send to dead letter queue.
     *
     * Important: This method is not supported by NATS.
     *
     * @param message   a message to return.
     */
    abandon(message) {
        return __awaiter(this, void 0, void 0, function* () {
            // Not supported
        });
    }
    /**
     * Permanently removes a message from the queue and sends it to dead letter queue.
     *
     * Important: This method is not supported by NATS.
     *
     * @param message   a message to be removed.
     */
    moveToDeadLetter(message) {
        return __awaiter(this, void 0, void 0, function* () {
            // Not supported
        });
    }
}
exports.NatsAbstractMessageQueue = NatsAbstractMessageQueue;
NatsAbstractMessageQueue._defaultConfig = pip_services3_commons_nodex_3.ConfigParams.fromTuples("subject", null, "queue_group", null, "options.serialize_envelop", true, "options.retry_connect", true, "options.connect_timeout", 0, "options.reconnect_timeout", 3000, "options.max_reconnect", 3, "options.flush_timeout", 3000);
//# sourceMappingURL=NatsAbstractMessageQueue.js.map