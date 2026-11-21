'use strict';

const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const fs = require('node:fs');
const path = require('node:path');
const { test } = require('node:test');
const vm = require('node:vm');

const SERVICE_TYPE = { DISCONNECT_REQUEST: 0x0209, CONNECTIONSTATE_REQUEST: 0x0207, TUNNELING_REQUEST: 0x0420 };
const RESPONSECODE = { E_CONNECTION_ID: 0x21 };

function load(name, dependencies, timers = { setTimeout, clearTimeout }) {
  const filename = path.join(__dirname, '../lib', name);
  const module = { exports: {} };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), {
    module, Buffer, AbortController, console, clearInterval, ...timers,
    require: (specifier) => {
      if (Object.hasOwn(dependencies, specifier)) return dependencies[specifier];
      return require(specifier);
    },
  }, { filename });
  return module.exports;
}

function simulator() {
  let now = 0;
  const timers = new Map();
  const clock = {
    setTimeout(callback, delay) {
      const key = Symbol();
      timers.set(key, { time: now + delay, callback });
      return key;
    },
    clearTimeout(key) {
      timers.delete(key);
    },
    tick(ms) {
      const end = now + ms;
      while (true) {
        const next = [...timers].sort((a, b) => a[1].time - b[1].time)[0];
        if (!next || next[1].time > end) break;
        const [key, { time, callback }] = next;
        timers.delete(key);
        now = time;
        callback();
      }
      now = end;
    },
  };
  const fixes = load('KNXConnectionFixes.js', {
    'knx/src/KnxConstants': { SERVICE_TYPE, RESPONSECODE },
    'knx/src/dptlib': { populateAPDU: (value, apdu) => { apdu.data = value; } },
  }, clock);
  const sent = [];
  const deferred = [];
  const callbacks = [];
  const fsm = {
    state: 'idle',
    useTunneling: true,
    channel_id: 7,
    seqnum: -1,
    log: { warn() {}, debug() {} },
    events: [],
    states: {
      connecting: { _onEnter() { this.emit('disconnected'); } },
      idle: {
        outbound_TUNNELING_REQUEST(datagram) { this.transition('sendDatagram', datagram); },
      },
      sendDatagram: {
        _onEnter(datagram) {
          datagram.tunnstate.seqnum = ++this.seqnum;
          this.send(datagram, (error) => {
            if (error) this.transition('idle');
            else this.transition('sendTunnReq_waitACK', datagram);
          });
        },
      },
    },
    clearQueue() { deferred.length = 0; },
    deferUntilTransition() {},
    prepareDatagram(service) {
      return { service, tunnstate: { channel_id: this.channel_id }, cemi: { apdu: {} } };
    },
    send(datagram, callback) {
      sent.push(datagram);
      if (callback) callbacks.push(callback);
    },
    transition(state, ...args) {
      this.states[this.state]._onExit?.call(this);
      this.state = state;
      this.states[state]._onEnter?.call(this, ...args);
      if (this.state === 'idle') {
        for (const [type, data] of deferred.splice(0)) this.handle(type, data);
      }
    },
    handle(type, data) {
      const handler = this.states[this.state][type];
      if (handler) handler.call(this, data);
      else deferred.push([type, data]);
    },
    emit(...args) { this.events.push(args); },
  };
  fixes.applyKNXConnectionFixes(fsm);
  return {
    fsm, clock, sent, callbacks, fixes,
    write(complete) {
      fixes.writeTunnelDatagram(fsm, '1/0/1', true, 'DPT1', new AbortController().signal, complete);
    },
    ack(status, seqnum = 0, channel = 7) {
      fsm.handle('inbound_TUNNELING_ACK', { tunnstate: { rsvd: status, seqnum, channel_id: channel } });
    },
  };
}

test('writes wait for the ACK and ignore ACKs for another sequence or channel', () => {
  const gateway = simulator();
  const completed = [];
  gateway.write((error) => completed.push(error));
  gateway.callbacks.shift()();
  gateway.ack(0, 1);
  gateway.ack(0, 0, 8);
  assert.equal(completed.length, 0);
  gateway.ack(0);
  assert.deepEqual(completed, [undefined]);
  assert.equal(gateway.fsm.state, 'idle');
});

for (const early of [true, false]) {
  test(`negative ${early ? 'early' : 'normal'} ACK rejects the write and releases the tunnel`, () => {
    const gateway = simulator();
    const completed = [];
    gateway.write((error) => completed.push(error));
    const sent = gateway.callbacks.shift();
    if (early) gateway.ack(0x21);
    sent();
    if (!early) gateway.ack(0x21);
    assert.match(completed[0].message, /TUNNELING_ACK rejected/);
    assert.equal(gateway.fsm.state, 'releasingHomeyTunnel');
    gateway.fsm.handle('inbound_DISCONNECT_RESPONSE', { connstate: { status: 0 } });
    gateway.clock.tick(1000);
    assert.equal(gateway.fsm.state, 'connecting');
  });
}

test('a lost ACK retries the same sequence and a second timeout reconnects without replaying queued work', () => {
  const gateway = simulator();
  const completed = [];
  gateway.write((error) => completed.push(error));
  gateway.callbacks.shift()();
  const previousRead = gateway.fsm.prepareDatagram(SERVICE_TYPE.TUNNELING_REQUEST);
  gateway.fsm.handle('outbound_TUNNELING_REQUEST', previousRead);
  gateway.clock.tick(2000);
  assert.equal(gateway.sent.length, 2);
  assert.equal(gateway.sent[0], gateway.sent[1]);
  assert.equal(gateway.sent[1].tunnstate.seqnum, 0);
  gateway.clock.tick(3000);
  assert.match(completed[0].message, /No TUNNELING_ACK/);
  assert.equal(gateway.fsm.state, 'connecting');
  gateway.fsm.transition('idle');
  gateway.fsm.handle('outbound_TUNNELING_REQUEST', previousRead);
  assert.equal(gateway.sent.length, 3);
  gateway.write((error) => completed.push(error));
  gateway.callbacks.shift()();
  gateway.ack(0, 1);
  assert.deepEqual(completed.slice(1), [undefined]);
});

test('heartbeat retries tolerate a late gateway response', () => {
  const gateway = simulator();
  gateway.fsm.transition('requestingConnState');
  gateway.clock.tick(10000);
  assert.equal(gateway.sent.length, 2);
  gateway.fsm.handle('inbound_CONNECTIONSTATE_RESPONSE', { connstate: { status: 0 } });
  gateway.clock.tick(10000);
  assert.equal(gateway.fsm.state, 'idle');
  assert.equal(gateway.sent.length, 2);
});

test('consecutive tunnel failures delay reconnection even after the gateway confirms closure', () => {
  const gateway = simulator();
  const fail = (seqnum, channel) => {
    gateway.write(() => {});
    gateway.callbacks.shift()();
    gateway.ack(0x21, seqnum, channel);
    gateway.fsm.handle('inbound_DISCONNECT_RESPONSE', { connstate: { status: 0 } });
  };
  fail(0, 7);
  gateway.clock.tick(1000);
  assert.equal(gateway.fsm.state, 'connecting');
  gateway.fsm.channel_id = 8;
  gateway.fsm.transition('idle');
  fail(1, 8);
  gateway.clock.tick(1000);
  assert.equal(gateway.fsm.state, 'releasingHomeyTunnel');
  gateway.clock.tick(1000);
  assert.equal(gateway.fsm.state, 'connecting');
});

function interfaceFixtures() {
  class SerialQueue {
    pending = Promise.resolve();

    add(run) {
      const operation = this.pending.then(run);
      this.pending = operation.catch(() => {});
      return operation;
    }
  }
  const connections = [];
  class Connection {
    constructor(options) {
      this.options = options;
      connections.push(this);
    }

    Connect() { this.connectCount = (this.connectCount || 0) + 1; }

    off() {}
  }
  class Socket extends EventEmitter {
    send(packet, port, address, callback) { callback(); }

    close() { this.closed = true; }
  }
  const KNXInterface = load('KNXInterface.js', {
    knx: { Connection },
    'promise-queue': SerialQueue,
    './KNXConnectionFixes': {
      applyKNXConnectionFixes() {},
      writeTunnelDatagram(connection, address, value, datatype, signal, complete) { connection.complete = complete; },
    },
  });
  return { KNXInterface, connections, Socket };
}

test('discovery reads the address, topology, MAC and friendly name from a complete search response', () => {
  const Manager = load('KNXInterfaceManager.js', { './KNXInterface': {} });
  const response = Buffer.alloc(68);
  response.set([6, 16, 2, 2, 0, 68, 8, 1, 192, 0, 2, 1]);
  response.writeUInt16BE(0x1234, 18);
  response.set([0, 17, 34, 51, 68, 85], 32);
  response.write('Test gateway', 38);
  const info = Manager.prototype.parseKNXResponse(response);
  assert.equal(info.interfaceIp, '192.0.2.1');
  assert.equal(info.knxAddress, '1.2.52');
  assert.equal(info.interfaceMac, '001122334455');
  assert.equal(info.interfaceName, 'Test gateway');
});

test('an IP change cancels active and queued writes and only opens the latest endpoint', async () => {
  const { KNXInterface, connections, Socket } = interfaceFixtures();
  const gateway = new KNXInterface({ interfaceIp: '192.0.2.1' });
  gateway._connectKNX();
  gateway._connectKNX();
  const [first] = connections;
  assert.equal(first.connectCount, 1);
  first.channel_id = 7;
  first.socket = new Socket();
  first.options.handlers.connected();

  const active = assert.rejects(gateway.writeKNXGroupAddress('1/0/1', true), /knx_no_connection/);
  const queued = assert.rejects(gateway.writeKNXGroupAddress('1/0/1', false), /knx_no_connection/);
  await Promise.resolve();
  assert.equal(typeof first.complete, 'function');
  gateway.updateIP('192.0.2.2');
  gateway.updateIP('192.0.2.3');
  first.socket.emit('message', Buffer.from([6, 16, 2, 10, 0, 8, 7, 0]), { address: '192.0.2.1' });
  await Promise.all([active, queued, gateway.reconnecting]);
  assert.equal(connections.length, 2);
  assert.equal(connections[1].options.ipAddr, '192.0.2.3');
  assert.equal(connections[1].connectCount, 1);
  await gateway.shutdown();
  assert.equal(gateway.knxConnection, null);
});

test('a saved tunnel clears only after a matching release confirmation', async () => {
  const { KNXInterface, connections, Socket } = interfaceFixtures();
  const Manager = load('KNXInterfaceManager.js', {
    './KNXInterface': KNXInterface,
  });
  const saved = {};
  const manager = new Manager('', {
    settings: { get: (key) => saved[key], set: (key, value) => { saved[key] = value; } },
  });
  const first = manager.createInterface({ interfaceIp: '192.0.2.1', interfaceMac: 'mac1' });
  const second = manager.createInterface({ interfaceIp: '192.0.2.2', interfaceMac: 'mac2' });
  manager.KNXInterfaces = { mac1: first, mac2: second };
  for (const connection of connections) {
    connection.channel_id = 7;
    connection.socket = new Socket();
    connection.options.handlers.connected();
  }
  const closing = manager.shutdown();
  const response = (status) => Buffer.from([6, 16, 2, 10, 0, 8, 7, status]);
  connections[0].socket.emit('message', response(0), { address: '192.0.2.1' });
  connections[1].socket.emit('message', response(0x22), { address: '192.0.2.2' });
  await closing;
  assert.ok(!saved.openTunnels.mac1);
  assert.equal(saved.openTunnels.mac2.channel, 7);
  assert.equal(saved.openTunnels.mac2.ip, '192.0.2.2');
});
