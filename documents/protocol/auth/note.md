- wireshark filter: `tcp.srcport == 3307 or tcp.dstport == 3307`
  
- mysql 8.0 `caching_sha2_password`
  - full auth: flush、mysqld启动等，要求client进行rsa或者加密连接传递password
  - fast auth: 走cache  
  
- wireshark坑点：其错误地将8.0 mysql server 回的包`data[4]` `0x01` 识别为`AuthSwitchRequest`,应该
为`AuthMoreData` ,data[5]
  - `0x03`  fast auth
  - `0x04`  full auth
  
- 正常`AuthSwitchRequest`包的`data[4]`为`0xfe`
  
- https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthMoreData
