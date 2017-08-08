mosquitto-1.4.11-opt是由jason.hou（email:houjixin@163.com）完成代码优化，并添加了部分新功能：
（1）优化mosquitto性能，采用了epoll代替poll、hash表加速订阅树遍历速度等措施，使得mosquitto的性能有了质的提升；
（2）增加了上下线通知的功能，即无需客户端做任何动作，优化后的mosquitto会在客户端连接建立和断开时向配置文件中指定的topic发送通知（通知会写明是哪个连接断开或者建立了）；
（3）增加了动态修改指定连接的keepalive的功能，为实现动态心跳打下了基础；
（4）增加了导出当前mosquitto中所有在线客户端的连接id的功能；
