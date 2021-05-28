const assert = require('chai').assert;

import { ConfigParams } from 'pip-services3-commons-nodex';

import { NatsConnectionResolver } from '../../src/connect/NatsConnectionResolver';

suite('NatsConnectionResolver', ()=> {

    test('Single Connection', async () => {
        let resolver = new NatsConnectionResolver();
        resolver.configure(ConfigParams.fromTuples(
            "connection.protocol", "nats",
            "connection.host", "localhost",
            "connection.port", 4222
        ));
    
        let connection = await resolver.resolve(null);
        assert.equal("localhost:4222", connection.getAsString("servers"));
        assert.isNull(connection.getAsString("username"));
        assert.isNull(connection.getAsString("password"));
        assert.isNull(connection.getAsString("token"));
    });

    test('Cluster Connection', async () => {
        let resolver = new NatsConnectionResolver();
        resolver.configure(ConfigParams.fromTuples(
            "connections.0.protocol", "nats",
            "connections.0.host", "server1",
            "connections.0.port", 4222,
            "connections.1.protocol", "nats",
            "connections.1.host", "server2",
            "connections.1.port", 4222,
            "connections.2.protocol", "nats",
            "connections.2.host", "server3",
            "connections.2.port", 4222,
        ));
    
        let connection = await resolver.resolve(null);
        assert.isNotNull(connection.getAsString("servers"));
        assert.isNull(connection.getAsString("username"));
        assert.isNull(connection.getAsString("password"));
        assert.isNull(connection.getAsString("token"));
    });

    test('Cluster Connection with Auth', async () => {
        let resolver = new NatsConnectionResolver();
        resolver.configure(ConfigParams.fromTuples(
            "connections.0.protocol", "nats",
            "connections.0.host", "server1",
            "connections.0.port", 4222,
            "connections.1.protocol", "nats",
            "connections.1.host", "server2",
            "connections.1.port", 4222,
            "connections.2.protocol", "nats",
            "connections.2.host", "server3",
            "connections.2.port", 4222,
            "credential.token", "ABC",
            "credential.username", "test",
            "credential.password", "pass123",
        ));
    
        let connection = await resolver.resolve(null);
        assert.isNotNull(connection.getAsString("servers"));
        assert.equal("test", connection.getAsString("username"));
        assert.equal("pass123", connection.getAsString("password"));
        assert.equal("ABC", connection.getAsString("token"));
    });

    test('Cluster URI', async () => {
        let resolver = new NatsConnectionResolver();
        resolver.configure(ConfigParams.fromTuples(
            "connection.uri", "nats://test:pass123@server1:4222,server2:4222,server3:4222?param=234",
        ));
    
        let connection = await resolver.resolve(null);
        assert.isNotNull(connection.getAsString("servers"));
        assert.equal("test", connection.getAsString("username"));
        assert.equal("pass123", connection.getAsString("password"));
        assert.isNull(connection.getAsString("token"));
    });

});