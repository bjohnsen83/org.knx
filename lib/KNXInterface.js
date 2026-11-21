'use strict';

const { EventEmitter } = require('events');
const dgram = require('dgram');
const knx = require('knx');
const PromiseQueue = require('promise-queue');
const { applyKNXConnectionFixes, writeTunnelDatagram } = require('./KNXConnectionFixes');

const READ_TIMEOUT_MS = 3000;
const WRITE_TIMEOUT_MS = 45000; // Includes time waiting for a heartbeat or an earlier read.

class KNXInterface extends EventEmitter {

  constructor(knxInterface) {
    super();

    // Setting object parameters from parameters from the interfacemanager
    this.name = knxInterface.interfaceName;
    this.ipAddress = knxInterface.interfaceIp;
    this.macAddress = knxInterface.interfaceMac;
    this.knxAddress = knxInterface.knxAddress;

    // KNX interface settings/variabeles
    this.isConnected = false; // Boolean to keep track of the connection status.
    this.isTimedOut = false; // Boolean to keep track of timeout events.
    this.knxDisconnectCount = 0;

    this.resetTunnelQueues();

    // Learnmode variables
    this.learnMode = false;
    this.knxEventCaptures = [];

    // Setup logging and binding for fucntions
    this.log = console.log.bind(this, (`[Interface: ${this.name}]`));

    // Object arrays with callbacks
    this.onKNXConnectionCallbacks = [];
    this.onKNXEventCallbacks = {};

    this.log(`Creating interface with IP ${this.ipAddress}`);
    this.createConnection();
  }

  // Function to create the KNX connection
  createConnection() {
    if (this.stopping) return;
    // The KNX tunnel connection itself through the knx library
    if (this.isConnected === true && !this.isTimedOut) return; // Skip re-initializing when already connected and healthy.
    // Dispose of a previous connection object so its handlers and timers can no longer
    // change the state of this interface.
    this._disposeConnection();
    try {
      this.knxConnection = new knx.Connection({
        ipAddr: this.ipAddress, // IP address obtained through interfacemanager
        ipPort: 3671, // Fixed and part of the KNXnet/IP protocol.
        physAddr: this.knxAddress, // KNX address obtained through interfacemanager
        forceTunneling: true, // We want true tunnel connections
        loglevel: 'info',
        manualConnect: true, // The connection should be only opened if it's going to be used.
        minimumDelay: 10, // This sets the timeout between messages in ms
        // Setup the handlers that the library will use to respond on emits from FSM.js
        suppress_ack_ldatareq: false,
        handlers: {
          connected: () => {
            this.log('KNX Connected');
            this.emit('tunnel_alive', this.knxConnection.channel_id, this.connectionIP);
            // Change both connection booleans to their optimal state
            this.isConnected = true;
            this.isTimedOut = false;

            this.onKNXConnectionCallbacks.forEach((connCallback) => {
              try {
                connCallback('connected');
              } catch (error) {
                this.log(error);
              }
            });
          },
          disconnected: () => {
            this.log('KNX Disconnected');
            this.isConnected = false;
            this.isTimedOut = false;
            this.resetTunnelQueues();

            this.onKNXConnectionCallbacks.forEach((callback) => {
              try {
                callback('disconnected');
              } catch (error) {
                this.log(error);
              }
            });
          },
          event: this.onKNXEventListener.bind(this),
          heartbeat_ok: () => {
            this.emit('tunnel_alive', this.knxConnection.channel_id, this.connectionIP);
          },
          tunnel_released: (channel) => {
            this.emit('tunnel_released', channel, this.connectionIP);
          },
          timeout: () => {
            this.isTimedOut = true;
            this.resetTunnelQueues();
            this.log('FSM timeout received');
          },
          error: (connstatus) => {
            this.log('Error from FSM:', connstatus);
          },
        },
      });
      this.connectionIP = this.ipAddress;
      applyKNXConnectionFixes(this.knxConnection);
    } catch (error) {
      this.log('KNX lib error', error); // This should be able to catch the 'no valid ipv4 interfaces' error
    }
  }

  // Remove all handlers from the current connection object, stop its timers and close
  // its socket.
  _disposeConnection(keepSocket = false) {
    const oldConnection = this.knxConnection;
    if (!oldConnection) return;
    this.knxConnection = null;
    try {
      oldConnection.off(); // machina: remove all event listeners
    } catch (error) {
      // ignore
    }
    ['idletimer', 'connstatetimer', 'disconnecttimer', 'tunnelingAckTimer', 'homeyReleaseTimer'].forEach((timer) => {
      clearTimeout(oldConnection[timer]);
    });
    clearInterval(oldConnection.connecttimer);
    // Pending callbacks (e.g. a scheduled reconnect) must not move it to another state.
    oldConnection.transition = () => {};
    oldConnection.handle = () => {};
    try {
      if (!keepSocket && oldConnection.socket) oldConnection.socket.close();
    } catch (error) {
      // socket may already be closed
    }
  }

  // ////////// KNX IP Interface and connection stuff ////////////

  // Function to (re)open the connection to the KNX network
  _connectKNX() {
    if (this.stopping || !this.knxConnection) return;
    // A tunnel left open by the previous run of the app (e.g. after a crash) is closed
    // first, so the interface does not have two tunnels from Homey at the same time.
    if (this.staleChannel !== undefined && this.staleChannel !== null) {
      if (this.releasingStaleChannel) return;
      this.releasingStaleChannel = true;
      const channel = this.staleChannel;
      this.staleChannel = null;
      this.log(`Closing tunnel ${channel} left open by the previous run`);
      const address = this.ipAddress;
      this.staleRelease = KNXInterface.sendDisconnect(address, channel, 1000)
        .catch((error) => {
          this.log('Closing old tunnel failed', error);
          return false;
        })
        .then((released) => {
          if (released) this.emit('tunnel_released', channel, address);
          this.releasingStaleChannel = false;
          this._connectKNX();
          return released;
        });
      return;
    }
    if (this.releasingStaleChannel) return;
    if (this.isConnected !== true) {
      if (this.connectStarted) return;
      this.log('Trying to (re)connect');
      try {
        this.connectStarted = true;
        this.knxConnection.Connect();
      } catch (error) {
        this.connectStarted = false;
        this.log('KNX lib connect error', error);
      }
    } else {
      this.log('Already connected');
    }
  }

  // Send a DISCONNECT_REQUEST for a tunnel channel from a temporary socket and wait for
  // the response (or the timeout).
  static sendDisconnect(ipAddress, channel, timeoutMs, socket = dgram.createSocket('udp4')) {
    return new Promise((resolve) => {
      let done = false;
      const finish = (result) => {
        if (done) return;
        done = true;
        clearTimeout(timer);
        try {
          socket.close();
        } catch (error) {
          // ignore
        }
        resolve(result);
      };
      const timer = setTimeout(() => finish(false), timeoutMs);
      socket.on('error', () => finish(false));
      socket.on('message', (msg, peer) => {
        if (peer.address !== ipAddress || msg.length !== 8 || msg[0] !== 6 || msg[1] !== 16
          || msg.readUInt16BE(2) !== 0x020a || msg.readUInt16BE(4) !== msg.length || msg[6] !== channel) return;
        // E_CONNECTION_ID also confirms the gateway no longer owns this channel.
        finish(msg[7] === 0 || msg[7] === 0x21);
      });
      // Header, channel, reserved, HPAI (UDP, 0.0.0.0:0 = reply to sender)
      const request = Buffer.from([0x06, 0x10, 0x02, 0x09, 0x00, 0x10, channel & 0xff, 0x00,
        0x08, 0x01, 0, 0, 0, 0, 0, 0]);
      try {
        socket.send(request, 3671, ipAddress, (error) => {
          if (error) finish(false);
        });
      } catch (error) {
        finish(false);
      }
    });
  }

  // Close the tunnel when the app stops, so the interface frees it right away instead of
  // keeping it for up to 120 s.
  async shutdown() {
    this.stopping = true;
    this.resetTunnelQueues();
    this.isConnected = false;
    await Promise.all([this.releaseCurrentTunnel(), this.reconnecting, this.staleRelease]);
  }

  async releaseCurrentTunnel() {
    const connection = this.knxConnection;
    const channel = connection?.channel_id;
    const address = this.connectionIP;
    const hasChannel = Number.isInteger(channel);
    this._disposeConnection(hasChannel);
    this.connectStarted = false;
    if (hasChannel && await KNXInterface.sendDisconnect(address, channel, 500, connection.socket)) {
      this.emit('tunnel_released', channel, address);
    }
  }

  // Function to update the IP address if a change is detected by the interfacemanager.
  updateIP(newIPaddress) {
    if (this.stopping) return;
    if (newIPaddress === this.ipAddress) {
      this.log('IP has not changed, maintaining current connection');
    } else {
      this.log('Creating new KNX tunnel connection');
      this.ipAddress = newIPaddress;
      if (this.connectStarted) this._reconnect();
      else if (!this.reconnecting) this.createConnection();
    }
  }

  // Safely disconnect and reconnect the KNX connection
  _reconnect() {
    if (this.reconnecting) return;
    this.isConnected = false;
    this.isTimedOut = false;
    this.resetTunnelQueues();
    this.onKNXConnectionCallbacks.forEach((callback) => {
      try {
        callback('disconnected');
      } catch (error) {
        this.log(error);
      }
    });
    this.reconnecting = this.releaseCurrentTunnel().catch((error) => {
      this.log('Closing old tunnel failed', error);
    }).finally(() => {
      this.reconnecting = null;
      this.createConnection();
      this._connectKNX();
    });
  }

  resetTunnelQueues() {
    if (this.tunnelWork) this.tunnelWork.abort();
    this.tunnelWork = new AbortController();
    // Bound the steady-state backlog while allowing large initial status refreshes.
    this.knxCommunicationQueue = new PromiseQueue(2, 1024);
    this.knxWriteQueue = new PromiseQueue(1, 128);
  }

  queueTunnelWork(queue, timeoutMs, timeoutMessage, start) {
    const { signal } = this.tunnelWork;
    const connection = this.knxConnection;
    return queue.add(() => new Promise((resolve, reject) => {
      if (signal.aborted) {
        reject(new Error('knx_no_connection'));
        return;
      }
      const work = new AbortController();
      const complete = (error, value) => {
        if (work.signal.aborted) return;
        work.abort();
        clearTimeout(timer);
        signal.removeEventListener('abort', abort);
        if (error) reject(error);
        else resolve(value);
      };
      const abort = () => complete(new Error('knx_no_connection'));
      const timer = setTimeout(() => complete(new Error(timeoutMessage)), timeoutMs);
      signal.addEventListener('abort', abort, { once: true });
      try {
        start(connection, complete, work.signal);
      } catch (error) {
        complete(error);
      }
    }));
  }

  // Function which returns the connected (or last connected) IP Address
  getConnectedIPAddress() {
    return this.ipAddress;
  }

  // KNX connection listener. Stores the given callback
  onKNXConnectionListener(callback) {
    if (this.isConnected) {
      callback('connected');
    }
    this.onKNXConnectionCallbacks.push(callback); // list with KNX callback to update on connecting.
    // console.log('KNXConnection callback list updated');
  }

  removeKNXConnectionListener(callback) {
    this.onKNXConnectionCallbacks = this.onKNXConnectionCallbacks.filter((cb) => cb !== callback);
    // console.log('KNXConnection callback list updated');
  }

  // KNX busevent listener, callbacks are added per groupaddress
  addKNXEventListener(groupaddress, callback) {
    if (typeof groupaddress === 'string' && groupaddress !== '') {
      if (!this.onKNXEventCallbacks[groupaddress]) {
        this.onKNXEventCallbacks[groupaddress] = [callback];
      } else {
        this.onKNXEventCallbacks[groupaddress].push(callback);
      }
    }
  }

  // Removes all listeners attached the the provided callback
  removeKNXEventListener(groupaddress, callback) {
    if (typeof groupaddress === 'string' && groupaddress !== '') {
      if (this.onKNXEventCallbacks[groupaddress]) {
        this.onKNXEventCallbacks[groupaddress] = this.onKNXEventCallbacks[groupaddress]
          .filter((cb) => cb !== callback);
      }
    }
  }

  // KNX busevent listener, triggered when an KNX event occurs
  onKNXEventListener(event, source, destination, value) {
    // Emit the event, and if there is a callback for the destiniation addres call it.
    if (!destination) return;
    this.emit('knx_event', event, destination, value);
    if (this.onKNXEventCallbacks[destination] && (event === 'GroupValue_Write' || event === 'GroupValue_Response')) {
      this.onKNXEventCallbacks[destination].forEach((callback) => {
        if (!callback || typeof callback !== 'function') return;
        callback(destination, value);
      });
    }
    // If the learnmode is turned on, also store the event in de capture array.
    if (this.learnMode) {
      this.log('Pushing event to learn mode store');
      this.knxEventCaptures.push({
        event,
        destination,
        source,
        value,
      });
    }
  }

  // ////////// KNX Group Address Stuff ////////////

  // Write a groupadress with the given value and datapoint type
  async writeKNXGroupAddress(groupaddress, value, datapoint) {
    // Check if the groupadress is not empty
    if (groupaddress && groupaddress !== '') {
      if (this.isConnected === true && !this.isTimedOut) {
        // Only add the request in the queue when the connection is openend.
        this.log(`Writing ${value} to ${groupaddress}`);
        return this.queueTunnelWork(this.knxWriteQueue, WRITE_TIMEOUT_MS, `knx_write_timeout: ${groupaddress}`,
          (connection, complete, signal) => writeTunnelDatagram(connection, groupaddress, value, datapoint, signal, complete));
      }
      ++this.knxDisconnectCount;
      throw new Error('knx_no_connection');
    }
    throw new Error('knx_no_groupaddress');
  }

  // Function to read the value for the given groupadress
  async readKNXGroupAddress(groupaddress) {
    if (groupaddress && groupaddress !== '') {
      // Check if the groupadress is not empty
      if (this.isConnected === true && !this.isTimedOut) {
        return this.queueTunnelWork(this.knxCommunicationQueue, READ_TIMEOUT_MS, `knx_read_timeout: ${groupaddress}`,
          (connection, complete) => connection.read(groupaddress, (src, data) => complete(null, data)));
      }
      ++this.knxDisconnectCount;
      throw new Error('knx_no_connection');
    }
    throw new Error('knx_no_groupaddress');
  }

  // Run the learnmode for the given time.
  async learnModeSwitch(time) {
    this.knxEventCaptures = [];
    if (this.learnMode === true) throw new Error('already_learmode');
    this.learnMode = true; // start the learnmode

    await this._wait(time);
    this.learnMode = false;
    return this.filterKNXEvents(this.knxEventCaptures);
  }

  /*
    Async wait function
   */
  _wait(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /* Function to filter the captured events from the learnmode.
  Duplicate entries will be reduced to one entry, which will be counted.
  The list will then be sorted by event occurrence if possible.
  */
  filterKNXEvents(list) {
    const filterSet = new Set();
    const countObj = {};
    for (const obj of list) {
      if (filterSet.has(obj.destination)) {
        countObj[obj.destination]++;
      } else {
        countObj[obj.destination] = 1;
        filterSet.add(obj.destination);
      }
    }
    list = list.sort((a, b) => {
      return (countObj[b.destination] - countObj[a.destination]);
    });
    let filteredList = [];
    list.forEach((event) => {
      if (event.value.readUInt8() === 0 || event.value.readUInt8() === 1) {
        filteredList.push(event);
      }
    });
    filteredList = filteredList.filter((event, pos, arr) => {
      return arr.map((mapObj) => mapObj['destination'])
        .indexOf(event['destination']) === pos;
    });
    return filteredList;
  }

}

module.exports = KNXInterface;
