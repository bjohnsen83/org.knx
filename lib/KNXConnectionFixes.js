'use strict';

/*
 * Runtime fixes for the tunneling state machine of the `knx` library (v2.5.4, the latest
 * release on npm, unchanged since 2024). They are applied to each connection instance,
 * so no patched copy of node_modules is needed.
 */

const KnxConstants = require('knx/src/KnxConstants');
const dptlib = require('knx/src/dptlib');

// KNXnet/IP spec: CONNECTIONSTATE_REQUEST_TIMEOUT = 10 s, up to 3 attempts.
const HEARTBEAT_TIMEOUT_MS = 10000;
const HEARTBEAT_ATTEMPTS = 3;
// Same value as the library uses. The spec says 1 s, which is too strict for some interfaces.
const TUNNELING_ACK_TIMEOUT_MS = 2000;
const tunnelGeneration = Symbol('tunnelGeneration');
const writeResult = Symbol('writeResult');
const writeSignal = Symbol('writeSignal');

function writeTunnelDatagram(fsm, address, value, datapoint, signal, complete) {
  if (value == null) throw new Error('knx_no_value');
  const datagram = fsm.prepareDatagram(KnxConstants.SERVICE_TYPE.TUNNELING_REQUEST);
  dptlib.populateAPDU(value, datagram.cemi.apdu, datapoint);
  datagram.cemi.dest_addr = address;
  datagram[writeResult] = complete;
  datagram[writeSignal] = signal;
  fsm.handle('outbound_TUNNELING_REQUEST', datagram);
}

function finishRequest(fsm, error) {
  const request = fsm.pendingTunnReq;
  fsm.pendingTunnReq = null;
  if (request && request[writeResult]) request[writeResult](error);
}

function acceptAck(fsm, ack) {
  if (ack.rsvd !== 0) {
    const reason = `TUNNELING_ACK rejected (status ${ack.rsvd})`;
    finishRequest(fsm, new Error(reason));
    releaseAndReconnect(fsm, reason);
    return;
  }
  fsm.homeyRecoveryAttempts = 0;
  finishRequest(fsm);
  fsm.transition('idle');
}

function confirmTunnelReleased(fsm) {
  fsm.emit('tunnel_released', fsm.channel_id);
  delete fsm.channel_id;
}

function sendRaw(fsm, buf) {
  try {
    fsm.socket.send(buf, 0, buf.length, fsm.remoteEndpoint.port, fsm.remoteEndpoint.addr.toString());
  } catch (error) {
    // socket may already be closed
  }
}

// Tell the interface we are leaving the current channel, then start a fresh connection.
// Without the DISCONNECT_REQUEST the interface keeps the old tunnel for up to 120 s; on
// interfaces with few tunnel slots the new connection is then rejected.
function releaseAndReconnect(fsm, reason) {
  fsm.log.warn(`KNX tunnel lost (${reason}), releasing channel ${fsm.channel_id} and reconnecting`);
  if (fsm.channel_id !== undefined && !Number.isNaN(Number(fsm.channel_id))) {
    try {
      fsm.send(fsm.prepareDatagram(KnxConstants.SERVICE_TYPE.DISCONNECT_REQUEST));
    } catch (error) {
      // best effort
    }
  }
  fsm.transition('releasingHomeyTunnel');
  fsm.emit('error', reason);
}

// Acknowledge and deliver an incoming L_Data.ind without leaving the current state.
function handleIndicationInPlace(fsm, datagram) {
  fsm.seqnumRecv = datagram.tunnstate.seqnum;
  fsm.acknowledge(datagram);
  fsm.emitEvent(datagram);
}

function applyKNXConnectionFixes(fsm) {
  if (!fsm || !fsm.states || fsm.homeyFixesApplied) return fsm;
  // eslint-disable-next-line no-param-reassign
  fsm.homeyFixesApplied = true;
  // Machina state definitions can be shared by connection instances.
  const states = Object.fromEntries(Object.entries(fsm.states).map(([name, state]) => [name, { ...state }]));
  fsm.states = states;
  const { TUNNELING_REQUEST } = KnxConstants.SERVICE_TYPE;
  fsm.homeyTunnelGeneration = 0;
  fsm.homeyRecoveryAttempts = 0;

  const originalPrepare = fsm.prepareDatagram;
  fsm.prepareDatagram = function prepareDatagram(service, ...args) {
    const datagram = originalPrepare.call(this, service, ...args);
    if (service === TUNNELING_REQUEST) datagram[tunnelGeneration] = this.homeyTunnelGeneration;
    return datagram;
  };

  const originalSend = fsm.send;
  fsm.send = function send(datagram, callback) {
    if (!callback || this.pendingTunnReq !== datagram) return originalSend.call(this, datagram, callback);
    return originalSend.call(this, datagram, (error) => {
      if (this.pendingTunnReq !== datagram) return;
      if (error) finishRequest(this, error);
      callback(error);
    });
  };

  const originalReceive = fsm.onUdpSocketMessage;
  fsm.onUdpSocketMessage = function receive(message, peer, ...args) {
    if (peer.address === this.remoteEndpoint.addr.toString()) {
      return originalReceive.call(this, message, peer, ...args);
    }
    return undefined;
  };

  function resetTunnelWork() {
    this.homeyTunnelGeneration += 1;
    this.clearQueue();
    this.pendingTunnReq = null;
    this.earlyTunnelAck = null;
    clearTimeout(this.idletimer);
    this.idletimer = null;
  }

  // Clear the idle timer and the heartbeat counter whenever a (re)connect starts. The
  // 60 s idle timer was never cleared, so it could fire in the middle of a new handshake.
  const originalConnectingEnter = states.connecting._onEnter;
  states.connecting._onEnter = function onEnterConnecting(...args) {
    resetTunnelWork.call(this);
    this.connStateAttempts = 0;
    return originalConnectingEnter.apply(this, args);
  };
  const originalHandshake = states.connecting.inbound_CONNECTIONSTATE_RESPONSE;
  states.connecting.inbound_CONNECTIONSTATE_RESPONSE = function onHandshake(datagram) {
    const status = datagram.connstate?.status;
    if (status === 0) originalHandshake.call(this, datagram);
    else if (status !== undefined) releaseAndReconnect(this, `KNX handshake rejected (status ${status})`);
  };

  states.releasingHomeyTunnel = {
    _onEnter() {
      resetTunnelWork.call(this);
      this.emit('disconnected');
      const delay = Math.min(1000 * (2 ** this.homeyRecoveryAttempts), 30000);
      this.homeyRecoveryAttempts = Math.min(this.homeyRecoveryAttempts + 1, 5);
      this.homeyReleaseTimer = setTimeout(() => this.transition('connecting'), delay);
    },
    inbound_DISCONNECT_RESPONSE(datagram) {
      const status = datagram.connstate?.status;
      if (status === 0 || status === KnxConstants.RESPONSECODE.E_CONNECTION_ID) {
        confirmTunnelReleased(this);
      }
    },
    _onExit() {
      clearTimeout(this.homeyReleaseTimer);
    },
  };

  states.idle._onEnter = function onEnterIdle() {
    if (!this.useTunneling || this.idletimer) return;
    this.idletimer = setTimeout(() => {
      this.idletimer = null;
      this.handle('homeyHeartbeatDue');
    }, 60000);
  };

  states.idle.homeyHeartbeatDue = function heartbeatDue() {
    this.transition('requestingConnState');
  };

  // Answer a DISCONNECT_REQUEST from the interface (the library did not send a
  // DISCONNECT_RESPONSE) before reconnecting.
  states.idle.inbound_DISCONNECT_REQUEST = function onDisconnectRequest(datagram) {
    if (!this.useTunneling) return;
    const channel = datagram && datagram.connstate ? datagram.connstate.channel_id : this.channel_id;
    sendRaw(this, Buffer.from([0x06, 0x10, 0x02, 0x0a, 0x00, 0x08, channel & 0xff, 0x00]));
    this.log.warn('KNX interface closed the tunnel, reconnecting');
    confirmTunnelReleased(this);
    this.transition('connecting');
    this.emit('error', 'KNX interface closed the tunnel');
  };

  // The library can reschedule outbound work outside its deferred queue.
  const originalOutbound = states.idle.outbound_TUNNELING_REQUEST;
  states.idle.outbound_TUNNELING_REQUEST = function outbound(datagram) {
    if (datagram[tunnelGeneration] !== this.homeyTunnelGeneration
      || datagram[writeSignal]?.aborted) return;
    originalOutbound.call(this, datagram);
  };

  const originalSendDatagramEnter = states.sendDatagram._onEnter;
  states.sendDatagram._onEnter = function onEnterSendDatagram(datagram, ...rest) {
    this.earlyTunnelAck = null;
    this.pendingTunnReq = datagram;
    return originalSendDatagramEnter.call(this, datagram, ...rest);
  };

  // The interface often answers before Node has run the UDP send callback, so the
  // TUNNELING_ACK arrives while still in `sendDatagram`. The library deferred it and then
  // waited in vain for an ACK it had already received. Remember it instead.
  states.sendDatagram.inbound_TUNNELING_ACK = function onEarlyAck(datagram) {
    const ack = datagram.tunnstate;
    if (ack && ack.seqnum === this.pendingTunnReq?.tunnstate?.seqnum && ack.channel_id === this.channel_id) {
      this.earlyTunnelAck = ack;
    }
  };

  states.sendDatagram['inbound_TUNNELING_REQUEST_L_Data.ind'] = function onIndication(datagram) {
    if (this.useTunneling) handleIndicationInPlace(this, datagram);
  };

  states.sendDatagram.inbound_DISCONNECT_REQUEST = states.idle.inbound_DISCONNECT_REQUEST;

  // TUNNELING_REQUEST without TUNNELING_ACK. The library just went back to idle with the
  // sequence counter already incremented. If the request never reached the interface,
  // the interface discards every following request (unexpected sequence number), so
  // Homey -> KNX silently stops working until the app is restarted. Per spec the request
  // is now repeated once with the same sequence number.
  states.sendTunnReq_waitACK = {
    _onEnter(datagram) {
      this.pendingTunnReq = datagram;
      if (this.earlyTunnelAck) {
        const ack = this.earlyTunnelAck;
        this.earlyTunnelAck = null;
        acceptAck(this, ack);
        return;
      }
      this.tunnelingAckTimer = setTimeout(() => {
        if (!datagram.homeyResent) {
          this.log.debug('No TUNNELING_ACK, repeating request once');
          // eslint-disable-next-line no-param-reassign
          datagram.homeyResent = true;
          this.transition('sendTunnReq_resend', datagram);
        } else {
          const reason = 'No TUNNELING_ACK after repeating the request';
          finishRequest(this, new Error(reason));
          this.emit('tunnelreqfailed', datagram);
          releaseAndReconnect(this, reason);
        }
      }, TUNNELING_ACK_TIMEOUT_MS);
    },
    _onExit() {
      clearTimeout(this.tunnelingAckTimer);
    },
    inbound_TUNNELING_ACK(datagram) {
      const ack = datagram.tunnstate;
      if (!ack || ack.seqnum !== this.pendingTunnReq?.tunnstate?.seqnum || ack.channel_id !== this.channel_id) return;
      acceptAck(this, ack);
    },
    'inbound_TUNNELING_REQUEST_L_Data.ind': function onIndication(datagram) {
      if (this.useTunneling) handleIndicationInPlace(this, datagram);
    },
    'inbound_TUNNELING_REQUEST_L_Data.con': function onConfirmation(datagram) {
      states.idle['inbound_TUNNELING_REQUEST_L_Data.con'].call(this, datagram);
    },
    inbound_DISCONNECT_REQUEST(datagram) {
      states.idle.inbound_DISCONNECT_REQUEST.call(this, datagram);
    },
    '*': function deferOther() {
      this.deferUntilTransition('idle');
    },
  };

  states.sendTunnReq_resend = {
    _onEnter(datagram) {
      this.send(datagram);
      this.transition('sendTunnReq_waitACK', datagram);
    },
  };

  // Heartbeat. The library waited only 1 s for the CONNECTIONSTATE_RESPONSE and gave up
  // after one attempt, so a single late or lost UDP packet tore down a healthy tunnel.
  // Incoming telegrams that arrive while waiting are now acknowledged and delivered
  // right away instead of being deferred.
  states.requestingConnState = {
    _onEnter() {
      this.connStateAttempts = (this.connStateAttempts || 0) + 1;
      this.send(this.prepareDatagram(KnxConstants.SERVICE_TYPE.CONNECTIONSTATE_REQUEST));
      this.connstatetimer = setTimeout(() => {
        if (this.connStateAttempts < HEARTBEAT_ATTEMPTS) {
          this.log.debug(`No heartbeat response (attempt ${this.connStateAttempts}), retrying`);
          this.transition('requestingConnStateRetry');
        } else {
          this.connStateAttempts = 0;
          releaseAndReconnect(this, 'timed out waiting for CONNECTIONSTATE_RESPONSE');
        }
      }, HEARTBEAT_TIMEOUT_MS);
    },
    _onExit() {
      clearTimeout(this.connstatetimer);
    },
    inbound_CONNECTIONSTATE_RESPONSE(datagram) {
      this.connStateAttempts = 0;
      if (datagram.connstate.status === 0) {
        this.transition('idle');
        this.emit('heartbeat_ok');
      } else {
        const state = KnxConstants.keyText('RESPONSECODE', datagram.connstate.status);
        if (datagram.connstate.status === KnxConstants.RESPONSECODE.E_CONNECTION_ID) {
          confirmTunnelReleased(this);
        }
        releaseAndReconnect(this, `KNX heartbeat rejected (${state})`);
      }
    },
    'inbound_TUNNELING_REQUEST_L_Data.ind': function onIndication(datagram) {
      if (this.useTunneling) handleIndicationInPlace(this, datagram);
    },
    'inbound_TUNNELING_REQUEST_L_Data.con': function onConfirmation(datagram) {
      states.idle['inbound_TUNNELING_REQUEST_L_Data.con'].call(this, datagram);
    },
    inbound_DISCONNECT_REQUEST(datagram) {
      states.idle.inbound_DISCONNECT_REQUEST.call(this, datagram);
    },
    '*': function deferOther() {
      this.deferUntilTransition('idle');
    },
  };

  // Intermediate state so a retry re-enters requestingConnState (machina ignores a
  // transition to the current state).
  states.requestingConnStateRetry = {
    _onEnter() {
      this.transition('requestingConnState');
    },
  };

  return fsm;
}

module.exports = { applyKNXConnectionFixes, writeTunnelDatagram };
