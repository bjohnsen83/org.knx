'use strict';

const { EventEmitter } = require('events');
const dgram = require('dgram');
const { isIPv4 } = require('net');
const KNXInterface = require('./KNXInterface');

// 3 minute search loop
const FIND_DEVICES_INTERVAL = 3 * (60 * 1000);
const RESPONSE_LENGTHS = new Map([[0x0202, 68], [0x0204, 60], [0x0206, 8], [0x0208, 8], [0x020a, 8]]);

class KNXInterfaceManager extends EventEmitter {

  constructor(localIP, homey) {
    super();

    this.homey = homey;

    // Formatted logging
    this.log = console.log.bind(this, '[KNX interface manager]');
    this.errorLog = console.error.bind(this, '[KNX interface manager] ERROR:');

    // search running status
    this.searchRunning = false;

    this.KNXInterfaces = {};

    if (isIPv4(localIP)) {
      this.localIPBuffer = Buffer.from(localIP.split('.').map(Number));
      const interfaces = this.homey.settings.get('interfaces') || [];

      this.findKNXInterfaces()
        .catch((error) => {
          this.log('findKNXinterfaces error', error);
        }).finally(async () => {
          for (const savedInterface of interfaces) {
            // Force recheck interfaces from previous session if they are not found by the UDP search
            if (savedInterface && savedInterface.mac && savedInterface.ip && !this.KNXInterfaces[savedInterface.mac]) {
              try {
                await this.discoverKNXInterfaceOnIP(savedInterface.ip);
              } catch (error) {
                this.errorLog('Error checking interface', savedInterface.ip, error);
              }
            }
          }
        });
    } else {
      this.errorLog('IP address is not a valid IPv4 format');
      return;
    }

    this.findDevicesInterval = setInterval(() => {
      this.findKNXInterfaces()
        .catch((error) => {
          this.log('findKNXinterfaces error', error);
        });
    }, FIND_DEVICES_INTERVAL);

    // Respond on an emit. Add the found interface to the list of interfaces.
    this.on('interface_found', (knxInterface) => {
      this.log('Current interfaces:', Object.values(this.KNXInterfaces).map((tempInterface) => {
        return tempInterface.name;
      }));
    });
  }

  // Clears the periodic discovery interval and any pending resources.
  destroy() {
    this.stopping = true;
    if (this.cancelDiscovery) this.cancelDiscovery();
    if (this.findDevicesInterval) {
      clearInterval(this.findDevicesInterval);
      this.findDevicesInterval = null;
    }
  }

  // Close all tunnels; called when the app stops.
  async shutdown() {
    this.destroy();
    await Promise.all(Object.values(this.KNXInterfaces).map(async (knxInterface) => {
      try {
        await knxInterface.shutdown();
      } catch (error) {
        this.errorLog('Closing tunnel failed', error);
      }
    }));
  }

  // ////////// KNX IP Interfaces ////////////

  // Creates a KNX interface. If the previous run of the app was stopped less than ~2
  // minutes ago without closing its tunnel (e.g. a crash), the interface still holds that
  // tunnel; it is closed before connecting.
  createInterface(knxInterfaceInfo) {
    const knxInterface = new KNXInterface(knxInterfaceInfo);
    const saved = (this.homey.settings.get('openTunnels') || {})[knxInterface.macAddress];
    if (saved && saved.ip === knxInterface.ipAddress && Number.isInteger(saved.channel)
      && saved.channel >= 0 && saved.channel <= 255 && Date.now() - saved.time < 150000) {
      knxInterface.staleChannel = saved.channel;
    }
    knxInterface.on('tunnel_alive', (channel, address) => {
      if (Number.isInteger(channel)) {
        this.saveOpenTunnel(knxInterface.macAddress, { ip: address, channel, time: Date.now() });
      }
    });
    knxInterface.on('tunnel_released', (channel, address) => {
      const current = (this.homey.settings.get('openTunnels') || {})[knxInterface.macAddress];
      if (current?.channel === channel && current.ip === address) {
        this.saveOpenTunnel(knxInterface.macAddress, null);
      }
    });
    return knxInterface;
  }

  // Remembers the open tunnel per interface (updated on every heartbeat).
  saveOpenTunnel(macAddress, info) {
    try {
      const tunnels = this.homey.settings.get('openTunnels') || {};
      if (info) tunnels[macAddress] = info;
      else delete tunnels[macAddress];
      this.homey.settings.set('openTunnels', tunnels);
    } catch (error) {
      this.errorLog('Saving tunnel info failed', error);
    }
  }

  // This method returns a KNXinterface with matching MAC address.
  getKNXInterface(macAddress) {
    return this.KNXInterfaces[macAddress];
  }

  // Thins method returns the complete list with KNX Interfaces
  getKNXInterfaceList() {
    return this.KNXInterfaces;
  }

  /**
   * Returns a simplified list of KNX interfaces used to store interfaces in the settings
   *
   * @returns {{ip: *, name: *, mac: *}[]}
   */
  getSimpleInterfaceList() {
    return Object.values(this.KNXInterfaces).map((knxInterface) => {
      return {
        name: knxInterface.name,
        mac: knxInterface.macAddress,
        ip: knxInterface.ipAddress,
      };
    });
  }

  // Check if a given IP is a KNX IP Interface
  async discoverKNXInterfaceOnIP(ipAddress) {
    // Check if the given address is a valid IPv4 address
    if (!isIPv4(ipAddress)) {
      throw new Error('invalid_ip');
    }
    return this.checkKNXInterface(ipAddress);
  }

  // Let the defined KNX Interface open the tunnel connection to the KNX network
  connectInterface(macAddress) {
    this.KNXInterfaces[macAddress]._connectKNX();
  }

  // Search for KNX interfaces (just a wrapper for this.app)
  searchInterfaces() {
    return this.findKNXInterfaces();
  }

  // Parse the received response for valid KNX IP data
  parseKNXResponse(inBuffer) {
    if (!Buffer.isBuffer(inBuffer) || inBuffer.length < 6
      || inBuffer.readUInt16BE(4) !== inBuffer.length) return null;
    const type = inBuffer.readUInt16BE(2);
    const minimumLength = RESPONSE_LENGTHS.get(type);
    if (minimumLength === undefined || inBuffer.length < minimumLength) return null;
    // Parse the first 2 bytes to check if it's KNXnet/IP traffic
    if (inBuffer[0] === 0x06 && inBuffer[1] === 0x10) {
      // Start checking the service types. Can be converted to switch case?
      // Search response
      if (type === 0x202) {
        // Obtain and parse the IP address from the interface
        const interfaceIpRaw = inBuffer.readUInt32LE(8);

        /* eslint-disable max-len */
        const interfaceIp = `${(interfaceIpRaw & 0xff).toString()}.${((interfaceIpRaw >> 8) & 0xff).toString()}.${((interfaceIpRaw >> 16) & 0xff).toString()}.${((interfaceIpRaw >> 24) & 0xff).toString()}`;
        /* eslint-enable */

        // Obtain and parse the KNX topology address from the interface
        const knxAddressRaw = inBuffer.readUInt16BE(18);
        const knxAddress = `${((knxAddressRaw & 0xf000) >> 12).toString()}.${((knxAddressRaw & 0x0f00) >> 8).toString()
        }.${(knxAddressRaw & 0xff).toString()}`;
        const interfaceMac = inBuffer.toString('hex', 32, 38); // Grab the macaddress bytes
        const interfaceName = inBuffer.toString('utf-8', 38, 68).replace(/\0[\s\S]*$/g, ''); // Read the fixed 30 bytes device description
        // this.log('Found', interfaceName, '@ IP:' + interfaceIp, 'with KNX address:', knxAddress);
        return {
          type: 0x202,
          interfaceName,
          interfaceIp,
          interfaceMac,
          knxAddress,
        }; // Return an object with all found values
      }

      // Description response
      if (type === 0x204) {
        // Obtain and parse the KNX topology Address
        const knxAddressRaw = inBuffer.readUInt16BE(10);
        const knxAddress = `${((knxAddressRaw & 0xf000) >> 12).toString()}.${((knxAddressRaw & 0x0f00) >> 8).toString()
        }.${(knxAddressRaw & 0xff).toString()}`;
        const interfaceMac = inBuffer.toString('hex', 24, 30); // Grab the macaddress bytes
        const interfaceName = inBuffer.toString('utf-8', 30, 60).replace(/\0[\s\S]*$/g, ''); // Read the fixed 30 bytes device description
        // this.log('Found', interfaceName, ' with KNX address:', knxAddress);
        return {
          type: 0x204,
          interfaceName,
          interfaceMac,
          knxAddress,
        }; // Return an object with all found values
      }
      // Connection response
      if (type === 0x206) {
        // Obtain the communictionchannel and the connection result
        const commChannel = inBuffer[6];// .readUInt16BE();
        const connectionResult = inBuffer[7];
        return { type: 0x206, commChannel, connectionResult };
      }
      // ConnectionState Response
      if (type === 0x208) {
        const commChannel = inBuffer[6];
        return { type: 0x208, commChannel };
      }
      // Disconnect response
      if (type === 0x020a) {
        return { type, commChannel: inBuffer[6], connectionResult: inBuffer[7] };
      }
    }
    // Non-KNX UDP packets are silently ignored to avoid log spam on noisy networks
    return null;
  }

  // Creates a UDP socket, then sends a KNX search request to the default KNX multicast IP.
  async findKNXInterfaces() {
    if (this.stopping) return;
    /* The correct flow to check if a given IP belongs to a KNX IP interface is:
    - Send an search request to the KNX multicast address.
    - Receive all search responses from the IP itnerfaces on the samen IP subnet.
    These actions mimics the traffic that ETS uses to search for IP interfaces
    */
    if (this.searchRunning === false) {
      this.log('Scanning for KNX interfaces in subnet');
      this.searchRunning = true;

      const knxIPPort = 3671; // Default KNX IP port, rarely changed
      const knxMultiCastAddress = '224.0.23.12'; // Default KNX multicast address
      const bufferByteLocalIP = this.localIPBuffer; // Grab the local IP address
      // 0x8F 0x66 for 36710. the port is just the default KNX port with a 0 added.
      const bufferByteSearchPort = Buffer.from([0x8F, 0x66]);

      // Create the KNX search request datagram
      const knxSearchRequest = Buffer.concat([Buffer.from(
        [0x06, 0x10, 0x2, 0x1, 0x00, 0x0e, 0x08, 0x01],
      ), // Fixed KNX search bytes
      bufferByteLocalIP,
      bufferByteSearchPort]);
      const udpSocket = dgram.createSocket('udp4'); // Create the socket connection

      return new Promise((resolve, reject) => {
        let finished = false;
        const finish = (error) => {
          if (finished) return;
          finished = true;
          clearTimeout(timer);
          this.cancelDiscovery = null;
          this.searchRunning = false;
          try {
            udpSocket.close();
          } catch (closeError) {
            // Socket may already be closed.
          }
          if (error) reject(error);
          else resolve('timeout');
        };
        this.cancelDiscovery = () => finish();
        const timer = setTimeout(() => {
          finish();
          if (Object.keys(this.KNXInterfaces).length === 0) this.emit('no_interfaces');
        }, 10000);
        udpSocket
          .on('error', finish)
          .on('message', (msg, rinfo) => {
            // When an message is received, parse it for KNXIP data
            const knxInterface = this.parseKNXResponse(msg);
            if (this.stopping || knxInterface?.type !== 0x0202
              || (knxInterface.interfaceIp !== rinfo.address && knxInterface.interfaceIp !== '0.0.0.0')) {
              return null; // Skip non-KNX messages, keep listening for valid responses
            }
            knxInterface.interfaceIp = rinfo.address;
            // If the data was valid, emit the found interface
            try {
              if (this.KNXInterfaces[knxInterface.interfaceMac]) {
                this.log('Updating', knxInterface.interfaceName);
                this.KNXInterfaces[knxInterface.interfaceMac].updateIP(knxInterface.interfaceIp);
              } else {
                this.log('New interface', knxInterface.interfaceName);
                // Use the Interface MAC as the key value
                this.KNXInterfaces[knxInterface.interfaceMac] = this.createInterface(knxInterface);
              }

              const interfaces = this.getSimpleInterfaceList();
              this.homey.settings.set('interfaces', interfaces);

              this.emit('interface_found', this.KNXInterfaces[knxInterface.interfaceMac]);
            } catch (error) {
              this.log('Creating IP interface instance failed: ', error);
            }
            return null;
          })
          .on('listening', () => {
            // Send the first message to trigger the searchrequest

            // Set the broadcast flag since the search request is using multicast.
            udpSocket.setBroadcast(true);
            udpSocket.send(knxSearchRequest, knxIPPort, knxMultiCastAddress, (error) => {
              if (error) finish(error);
            });
          })
          .bind(36710);
      });
    }
    throw new Error('search_already_running');
  }

  async checkKNXInterface(ipAddress) {
    if (this.stopping) return;
    /* The correct flow to check if a given IP belongs to a KNX IP interface is:
    - Send an connection request. If this gets accepted it's certainly a KNX IP interface
    - Send the description request  to obtain the device information
    These actions mimics the traffic that ETS uses to check a IP interface
    */
    if (this.searchRunning === false) {
      this.log('Checking if', ipAddress, 'is a KNX IP interface');
      this.searchRunning = true;

      const knxIPPort = 3671; // Default KNX IP port, rarely changed
      const bufferByteLocalIP = this.localIPBuffer; // Grab the local IP address
      const bufferByteConnectionPort = Buffer.from([0x8F, 0x67]); // port 36711
      const bufferByteDevicePort = Buffer.from([0x8F, 0x68]); // port 36712

      // KNX header for connect request, 8 octets, IPV4
      const knxConnectRequest = Buffer.concat(
        [Buffer.from([0x06, 0x10, 0x02, 0x05, 0x00, 0x1a, 0x08, 0x01]),
          bufferByteLocalIP, // IP for HPAI discovery
          bufferByteConnectionPort, // Port for HPAI discovery
          Buffer.from([0x08, 0x01]), // 8 octets, IPV4
          bufferByteLocalIP, // IP for HPAI data endpoint
          bufferByteDevicePort, // Port for HPAI data endpoint
          // 4octets, Tunnel connection, tunnel linklayer, 00 reserved
          Buffer.from([0x04, 0x04, 0x02, 0x00])],
      );

      const knxDeviceInfoRequest = Buffer.concat(
        [Buffer.from([0x06, 0x10, 0x02, 0x03, 0x00, 0x0e, 0x08, 0x01]), // KNX header
          bufferByteLocalIP,
          bufferByteConnectionPort],
      );

      const udpSocket = dgram.createSocket('udp4'); // Create the socket connections

      return new Promise((resolve, reject) => {
        let probeChannel;
        let descriptionRequested = false;
        let finished = false;
        const finish = async (error, description) => {
          if (finished) return;
          finished = true;
          clearTimeout(timer);
          try {
            if (probeChannel !== undefined) {
              await KNXInterface.sendDisconnect(ipAddress, probeChannel, 500, udpSocket);
            } else {
              udpSocket.close();
            }
          } catch (closeError) {
            this.errorLog('Closing probe socket failed', closeError);
          } finally {
            this.cancelDiscovery = null;
            this.searchRunning = false;
          }
          if (this.stopping) {
            resolve();
            return;
          }
          if (error) {
            reject(error);
            return;
          }
          try {
            const info = { ...description, interfaceIp: ipAddress };
            const existing = this.KNXInterfaces[info.interfaceMac];
            if (existing) existing.updateIP(ipAddress);
            else this.KNXInterfaces[info.interfaceMac] = this.createInterface(info);
            const found = this.KNXInterfaces[info.interfaceMac];
            this.homey.settings.set('interfaces', this.getSimpleInterfaceList());
            this.emit('interface_found', found);
            resolve(found);
          } catch (updateError) {
            reject(updateError);
          }
        };
        const send = (request) => {
          udpSocket.send(request, knxIPPort, ipAddress, (error) => {
            if (error) finish(error);
          });
        };
        this.cancelDiscovery = () => finish();
        const timer = setTimeout(() => finish(new Error('interface_not_found')), 5000);

        udpSocket
          .on('error', finish)
          .on('message', (msg, rinfo) => {
            if (finished || this.stopping || rinfo.address !== ipAddress) return;
            const commResult = this.parseKNXResponse(msg);
            if (commResult?.type === 0x206 && !descriptionRequested) {
              descriptionRequested = true;
              if (commResult.connectionResult === 0x00) probeChannel = commResult.commChannel;
              send(knxDeviceInfoRequest);
            } else if (commResult?.type === 0x204 && descriptionRequested) {
              finish(null, commResult);
            }
          })
          .on('listening', () => send(knxConnectRequest))
          .bind(36711);
      });
    }
    throw new Error('search_already_running');
  }

}

module.exports = KNXInterfaceManager;
