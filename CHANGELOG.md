# MICRO-MQ

## v0.3.8 - 20240229

### Feat

- upgrade fastapi:v0.2.2 and functools:v0.2.4.

## v0.3.7 - 20231020

### Fix

- 修复跨域访问的错误;

## v0.3.6 - 20231017

### Feat

- 新增获取broker状态信息接口;
- 支持跨域访问;

## v0.3.5 - 20230913

### Feat

- 新增edge接口；

## v0.3.4 - 20230821

### Fix

- 注册消息加密支持;
- 支持对所有传输消息进行加密;
- 响应不加密;

## v0.3.3 - 20230820

### Feat

- 注册消息加密支持;
- 支持对所有传输消息进行加密;
- 注册响应不加密;
- 修改`Message`接口和消息编解码方式;
- 修改`HistoryRecord`;

## v0.3.3-pre2 - 20230816

### Refactor

- 修改`Message`接口;
- 修改消息编解码方式，统一通过`Frame`实现;

## v0.3.3-pre1 - 20230816

## Breaking - 20230819

- 删除`ReRegisterMessageType`, 合并`MessageResponse.Status`到中;

### Feat

- 加密注册消息;
- 修改默认的SHA算法为SHA-256;

## v0.3.2 - 20230815

### Feat

- `Monitor`操作加锁；
- 修改SDK以增加心跳上报；

## v0.3.1-pre7 - 20230812

### Fix

- 修改超时检测任务；

## v0.3.1-pre6 - 20230812

## Breaking

- 修改环境变量名称

### Feat

- 检测客户端注册超时和心跳超时事件;

## v0.3.1-pre5 - 20230812

### Feat

- 新增接口`transfer.Conn`以分离`engine`与`transfer`;

## v0.3.1-pre4 - 20230809

### Refactor

- rename `engine.emptyEventHandler` to `engine.DefaultEventHandler`;

### Feat

- 新增心跳保活，客户端需要按照`ResponseMessage`中的参数发送心跳。

## v0.3.1-pre3 - 20230808

### Feat

- 引入`fastapi`服务；
- 增加`edge`接口，允许通过`HTTP`接口向Broker发送生产者消息；

## Breaking

- 修改`MQ`相关环境变量为`BROKER`名称；

## v0.3.1 - 20230806

### Feat

- [230804] `RegisterMessage`新增`token`字段以支持在注册时进行认证;
- [230804] 同步修改sdk和test文件;
- [230806] 新增说明文档;
- [230806] 新增链式处理message的方法以降低后续修改的复杂度;
- [230806] 修改注册为链式处理，pm的链式处理暂未上线;

## v0.3.0 - 20230801

## Breaking

- rename `synshare-mq` to `micromq`

## v0.2.2 - 20230726

### Feat

- update `functools:v0.2.0`

## v0.2.0 - 20230723

### Feat

- add `MQ`;

## v0.1.5 - 20230723

### Fix

- 修复`CM`.`Offset`和`ProductTime`缺少一个字节的错误;

## v0.1.4 - 20230723

### Fix

- 修复`Engine.AddTopic`存在的覆盖行为;