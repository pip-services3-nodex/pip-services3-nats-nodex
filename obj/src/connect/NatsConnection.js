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
exports.NatsConnection = void 0;
/** @module connect */
/** @hidden */
const nats = require('nats');
/** @hidden */
const os = require('os');
const pip_services3_commons_nodex_1 = require("pip-services3-commons-nodex");
const pip_services3_commons_nodex_2 = require("pip-services3-commons-nodex");
const pip_services3_commons_nodex_3 = require("pip-services3-commons-nodex");
const pip_services3_components_nodex_1 = require("pip-services3-components-nodex");
const NatsConnectionResolver_1 = require("./NatsConnectionResolver");
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
class NatsConnection {
    /**
     * Creates a new instance of the connection component.
     */
    constructor() {
        this._defaultConfig = pip_services3_commons_nodex_1.ConfigParams.fromTuples(
        // connections.*
        // credential.*
        "client_id", null, "options.retry_connect", true, "options.connect_timeout", 0, "options.reconnect_timeout", 3000, "options.max_reconnect", 3, "options.flush_timeout", 3000);
        /**
         * The logger.
         */
        this._logger = new pip_services3_components_nodex_1.CompositeLogger();
        /**
         * The connection resolver.
         */
        this._connectionResolver = new NatsConnectionResolver_1.NatsConnectionResolver();
        /**
         * The configuration options.
         */
        this._options = new pip_services3_commons_nodex_1.ConfigParams();
        /**
         * Topic subscriptions
         */
        this._subscriptions = [];
        this._clientId = os.hostname();
        this._retryConnect = true;
        this._maxReconnect = 3;
        this._reconnectTimeout = 3000;
        this._flushTimeout = 3000;
    }
    /**
     * Configures component by passing configuration parameters.
     *
     * @param config    configuration parameters to be set.
     */
    configure(config) {
        config = config.setDefaults(this._defaultConfig);
        this._connectionResolver.configure(config);
        this._options = this._options.override(config.getSection("options"));
        this._clientId = config.getAsStringWithDefault("client_id", this._clientId);
        this._retryConnect = config.getAsBooleanWithDefault("options.retry_connect", this._retryConnect);
        this._maxReconnect = config.getAsIntegerWithDefault("options.max_reconnect", this._maxReconnect);
        this._reconnectTimeout = config.getAsIntegerWithDefault("options.reconnect_timeout", this._reconnectTimeout);
        this._flushTimeout = config.getAsIntegerWithDefault("options.flush_timeout", this._flushTimeout);
    }
    /**
     * Sets references to dependent components.
     *
     * @param references 	references to locate the component dependencies.
     */
    setReferences(references) {
        this._logger.setReferences(references);
        this._connectionResolver.setReferences(references);
    }
    /**
     * Checks if the component is opened.
     *
     * @returns true if the component has been opened and false otherwise.
     */
    isOpen() {
        return this._connection != null;
    }
    /**
     * Opens the component.
     *
     * @param correlationId 	(optional) transaction id to trace execution through call chain.
     */
    open(correlationId) {
        return __awaiter(this, void 0, void 0, function* () {
            if (this._connection != null) {
                return;
            }
            let config = yield this._connectionResolver.resolve(correlationId);
            try {
                let options = {
                    "name": this._clientId,
                    "reconnect": this._retryConnect,
                    "maxReconnectAttempts": this._maxReconnect,
                    "reconnectTimeWait": this._reconnectTimeout
                };
                let servers = config.getAsString("servers");
                servers = servers.split(",");
                options["servers"] = servers;
                let username = config.getAsString("username");
                let password = config.getAsString("password");
                if (username != null) {
                    options["username"] = username;
                    options["password"] = password;
                }
                let token = config.getAsString("token");
                if (token != null) {
                    options["token"] = token;
                }
                this._connection = yield nats.connect(options);
                this._logger.debug(correlationId, "Connected to NATS server at " + servers);
            }
            catch (ex) {
                this._logger.error(correlationId, ex, "Failed to connect to NATS server");
                let err = new pip_services3_commons_nodex_2.ConnectionException(correlationId, "CONNECT_FAILED", "Connection to NATS service failed").withCause(ex);
                throw err;
            }
        });
    }
    /**
     * Closes component and frees used resources.
     *
     * @param correlationId 	(optional) transaction id to trace execution through call chain.
     */
    close(correlationId) {
        return __awaiter(this, void 0, void 0, function* () {
            if (this._connection == null) {
                return;
            }
            this._connection.close();
            this._connection = null;
            this._subscriptions = [];
            this._logger.debug(correlationId, "Disconnected from NATS server");
        });
    }
    getConnection() {
        return this._connection;
    }
    /**
     * Reads a list of registered queue names.
     * If connection doesn't support this function returnes an empty list.
     * @returns a list with registered queue names.
     */
    readQueueNames() {
        return __awaiter(this, void 0, void 0, function* () {
            // Not supported
            return [];
        });
    }
    /**
     * Creates a message queue.
     * If connection doesn't support this function it exists without error.
     * @param name the name of the queue to be created.
     */
    createQueue(name) {
        return __awaiter(this, void 0, void 0, function* () {
            // Not supported
        });
    }
    /**
     * Deletes a message queue.
     * If connection doesn't support this function it exists without error.
     * @param name the name of the queue to be deleted.
     */
    deleteQueue(name) {
        return __awaiter(this, void 0, void 0, function* () {
            // Not supported
        });
    }
    /**
     * Checks if connection is open
     * @returns an error is connection is closed or <code>null<code> otherwise.
     */
    checkOpen() {
        if (this.isOpen())
            return;
        throw new pip_services3_commons_nodex_3.InvalidStateException(null, "NOT_OPEN", "Connection was not opened");
    }
    /**
     * Publish a message to a specified topic
     * @param subject a subject(topic) where the message will be placed
     * @param message a message to be published
     */
    publish(subject, message) {
        return __awaiter(this, void 0, void 0, function* () {
            // Check for open connection
            this.checkOpen();
            subject = subject || message.subject;
            this._connection.publish(subject, message.data, { headers: message.headers });
        });
    }
    /**
     * Subscribe to a topic
     * @param subject a subject(topic) name
     * @param options subscription options
     * @param listener a message listener
     */
    subscribe(subject, options, listener) {
        return __awaiter(this, void 0, void 0, function* () {
            // Check for open connection
            this.checkOpen();
            // Subscribe to topic
            let handler = this._connection.subscribe(subject, {
                max: options.max,
                timeout: options.timeout,
                queue: options.queue,
                callback: (err, message) => {
                    listener.onMessage(err, message);
                }
            });
            // Determine if messages shall be filtered (topic without wildcarts)
            let filter = subject.indexOf("*") < 0;
            // Add the subscription
            let subscription = {
                subject: subject,
                options: options,
                filter: filter,
                handler: handler,
                listener: listener
            };
            this._subscriptions.push(subscription);
        });
    }
    /**
     * Unsubscribe from a previously subscribed topic
     * @param subject a subject(topic) name
     * @param listener a message listener
     */
    unsubscribe(subject, listener) {
        return __awaiter(this, void 0, void 0, function* () {
            // Find the subscription index
            let index = this._subscriptions.findIndex((s) => s.subject == subject && s.listener == listener);
            if (index < 0) {
                return;
            }
            // Remove the subscription
            let subscription = this._subscriptions.splice(index, 1)[0];
            // Unsubscribe from the topic
            if (this.isOpen() && subscription.handler != null) {
                subscription.handler.unsubscribe();
            }
        });
    }
}
exports.NatsConnection = NatsConnection;
//# sourceMappingURL=NatsConnection.js.map