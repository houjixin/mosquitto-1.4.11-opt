mosquitto-1.4.11-opt是由jason.hou（email:houjixin@163.com）完成代码优化，并添加了部分新功能：
（1）优化mosquitto性能，采用了epoll代替poll、hash表加速订阅树遍历速度等措施，使得mosquitto的性能有了质的提升；
（2）增加了上下线通知的功能，即无需客户端做任何动作，优化后的mosquitto会在客户端连接建立和断开时向配置文件中指定的topic发送通知（通知会写明是哪个连接断开或者建立了）；
（3）增加了动态修改指定连接的keepalive的功能，为实现动态心跳打下了基础；
（4）增加了导出当前mosquitto中所有在线客户端的连接id的功能；


Eclipse Mosquitto
=================

Mosquitto is an open source implementation of a server for version 3.1 and
3.1.1 of the MQTT protocol. It also includes a C and C++ client library, and
the `mosquitto_pub` and `mosquitto_sub` utilities for publishing and
subscribing.

## Links

See the following links for more information on MQTT:

- Community page: <http://mqtt.org/>
- MQTT v3.1.1 standard: <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html>

Mosquitto project information is available at the following locations:

- Main homepage: <http://mosquitto.org/>
- Find existing bugs or submit a new bug: <https://github.com/eclipse/mosquitto/issues>
- Source code repository: <https://github.com/eclipse/mosquitto>

There is also a public test server available at <http://test.mosquitto.org/>

## Installing

See <http://mosquitto.org/download/> for details on installing binaries for
various platforms.

## Quick start

If you have installed a binary package the broker should have been started
automatically. If not, it can be started with a basic configuration:

    mosquitto

Then use `mosquitto_sub` to subscribe to a topic:

    mosquitto_sub -t 'test/topic' -v

And to publish a message:

    mosquitto_pub -t 'test/topic' -m 'hello world'

## Documentation

Documentation for the broker, clients and client library API can be found in
the man pages, which are available online at <http://mosquitto.org/man/>. There
are also pages with an introduction to the features of MQTT, the
`mosquitto_passwd` utility for dealing with username/passwords, and a
description of the configuration file options available for the broker.

Detailed client library API documentation can be found at <http://mosquitto.org/api/>

## Building from source

To build from source the recommended route for end users is to download the
archive from <http://mosquitto.org/download/>.

On Windows and Mac, use `cmake` to build. On other platforms, just run `make`
to build. For Windows, see also `readme-windows.md`.

If you are building from the git repository then the documentation will not
already be built. Use `make binary` to skip building the man pages, or install
`docbook-xsl` on Debian/Ubuntu systems.

### Build Dependencies

* c-ares (libc-ares-dev on Debian based systems) - disable with `make WITH_DNS_SRV=no`
* libuuid (uuid-dev) - disable with `make WITH_UUID=no`
* libwebsockets (libwebsockets-dev) - enable with `make WITH_LIBWEBSOCKETS=yes`
* openssl (libssl-dev on Debian based systems) - disable with `make WITH_TLS=no`

## Credits

Mosquitto was written by Roger Light <roger@atchoo.org>

Master: [![Travis Build Status (master)](https://travis-ci.org/eclipse/mosquitto.svg?branch=master)](https://travis-ci.org/eclipse/mosquitto)
Develop: [![Travis Build Status (develop)](https://travis-ci.org/eclipse/mosquitto.svg?branch=develop)](https://travis-ci.org/eclipse/mosquitto)
Fixes: [![Travis Build Status (fixes)](https://travis-ci.org/eclipse/mosquitto.svg?branch=fixes)](https://travis-ci.org/eclipse/mosquitto)
