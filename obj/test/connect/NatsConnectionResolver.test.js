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
const assert = require('chai').assert;
const pip_services3_commons_nodex_1 = require("pip-services3-commons-nodex");
const NatsConnectionResolver_1 = require("../../src/connect/NatsConnectionResolver");
suite('NatsConnectionResolver', () => {
    test('Single Connection', () => __awaiter(void 0, void 0, void 0, function* () {
        let resolver = new NatsConnectionResolver_1.NatsConnectionResolver();
        resolver.configure(pip_services3_commons_nodex_1.ConfigParams.fromTuples("connection.protocol", "nats", "connection.host", "localhost", "connection.port", 4222));
        let connection = yield resolver.resolve(null);
        assert.equal("localhost:4222", connection.getAsString("servers"));
        assert.isNull(connection.getAsString("username"));
        assert.isNull(connection.getAsString("password"));
        assert.isNull(connection.getAsString("token"));
    }));
    test('Cluster Connection', () => __awaiter(void 0, void 0, void 0, function* () {
        let resolver = new NatsConnectionResolver_1.NatsConnectionResolver();
        resolver.configure(pip_services3_commons_nodex_1.ConfigParams.fromTuples("connections.0.protocol", "nats", "connections.0.host", "server1", "connections.0.port", 4222, "connections.1.protocol", "nats", "connections.1.host", "server2", "connections.1.port", 4222, "connections.2.protocol", "nats", "connections.2.host", "server3", "connections.2.port", 4222));
        let connection = yield resolver.resolve(null);
        assert.isNotNull(connection.getAsString("servers"));
        assert.isNull(connection.getAsString("username"));
        assert.isNull(connection.getAsString("password"));
        assert.isNull(connection.getAsString("token"));
    }));
    test('Cluster Connection with Auth', () => __awaiter(void 0, void 0, void 0, function* () {
        let resolver = new NatsConnectionResolver_1.NatsConnectionResolver();
        resolver.configure(pip_services3_commons_nodex_1.ConfigParams.fromTuples("connections.0.protocol", "nats", "connections.0.host", "server1", "connections.0.port", 4222, "connections.1.protocol", "nats", "connections.1.host", "server2", "connections.1.port", 4222, "connections.2.protocol", "nats", "connections.2.host", "server3", "connections.2.port", 4222, "credential.token", "ABC", "credential.username", "test", "credential.password", "pass123"));
        let connection = yield resolver.resolve(null);
        assert.isNotNull(connection.getAsString("servers"));
        assert.equal("test", connection.getAsString("username"));
        assert.equal("pass123", connection.getAsString("password"));
        assert.equal("ABC", connection.getAsString("token"));
    }));
    test('Cluster URI', () => __awaiter(void 0, void 0, void 0, function* () {
        let resolver = new NatsConnectionResolver_1.NatsConnectionResolver();
        resolver.configure(pip_services3_commons_nodex_1.ConfigParams.fromTuples("connection.uri", "nats://test:pass123@server1:4222,server2:4222,server3:4222?param=234"));
        let connection = yield resolver.resolve(null);
        assert.isNotNull(connection.getAsString("servers"));
        assert.equal("test", connection.getAsString("username"));
        assert.equal("pass123", connection.getAsString("password"));
        assert.isNull(connection.getAsString("token"));
    }));
});
//# sourceMappingURL=NatsConnectionResolver.test.js.map