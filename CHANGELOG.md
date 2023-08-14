# MICRO-MQ

## v0.3.1-pre7 - 20230812

### Feat

- 修改SDK以增加心跳上报；

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