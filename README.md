# micro-mq

- 可运行在IoT设备中的MQ-Broker,同时包含`Broker`和`Client`.
- 简单的消息中间件实现.
- 实时的消息投递,不支持`Client`指定`offset`.

## 依赖

- [functools](https://github.com/Chendemo12/functools) 构建TCP；
- [fastapi](https://github.com/Chendemo12/fastapi) 构建edge；

## 项目背景

本人有4台Linux设备，其中大部分都处于`7x24`工作的状态，而且分工明确，随着工作时间和服务的增加，维护成本也越来越高。
且设备之前需要彼此通信，存在着数据交叉备份以及功能依赖的情况，前期通过`API`（类似于配置中心）的形式获取事件的方式随着机器数量的增加逐渐变得难以维护。
因此有了通过消息中间件订阅的形式来解决（事件驱动）的想法。
为此我想到了`kafka`和`RabbitMQ`，但是`kafka`太重，难以在IoT设备中运行，`RabbitMQ`的内存占用也较大；随后在考虑`MQTT`时发现我不会用。
考虑到之前已有基础实现且功能也简单明确，因此有了此项目。

此项目实现了：

- MQ-Broker：异步非阻塞；
- TCP-Producer：异步长连接生产者;
- TCP-Consumer：异步长连接消费者;
- HTTP-Producer (edge)：同步HTTP生产者，灵感来自MQTT;
- 低至10M以下的内存占用，启动需要约3M的内存（非专业测试）；

目前尚有不足，但满足基本使用，后期考虑增加`Prometheus-exporter`和`web-api`。

## Usage

- 项目基于Golang；
- 目前提供go-sdk，python-sdk在开发过程中;
- 关于生产者和消费者的实现可参考 ["https://github.com/Chendemo12/mqlistener"](https://github.com/Chendemo12/mqlistener)

### Broker/Server

server分为`engine`和`transfer`2部分，其中`engine`负责事件处理，`transfer`则负责数据传输，目前实现了`tcp-transfer`
和`http-transfer`;

采用切面编程思想，可方便的修改`transfer`实现。
其中传输协议位于`proto`，集体实现可参考`mq.MQ`, 注意`token`的设置。

edge 自带`swagger`文档，路由绑定`/docs`。

```go

package main

import (
	"github.com/Chendemo12/functools/environ"
	"github.com/Chendemo12/functools/zaplog"
	"github.com/Chendemo12/micromq/src/mq"
	"github.com/Chendemo12/micromq/src/proto"
)

func main() {
	conf := mq.DefaultConf()

	conf.AppName = environ.GetString("APP_NAME", "micromq")
	conf.Version = "1.0.0"
	conf.Debug = environ.GetBool("DEBUG", false)

	conf.Broker.Host = environ.GetString("BROKER_LISTEN_HOST", "0.0.0.0")
	conf.Broker.Port = environ.GetString("BROKER_LISTEN_PORT", "7270")
	conf.Broker.BufferSize = environ.GetInt("BROKER_BUFFER_SIZE", 100)
	conf.Broker.MaxOpenConn = environ.GetInt("BROKER_MAX_OPEN_SIZE", 50)
	conf.Broker.HeartbeatTimeout = float64(environ.GetInt("BROKER_HEARTBEAT_TIMEOUT", 60))
	conf.Broker.Token = proto.CalcSHA(environ.GetString("BROKER_TOKEN", ""))
	// 是否开启消息加密
	msgEncrypt := environ.GetBool("BROKER_MESSAGE_ENCRYPT", false)
	// 消息加密方案, 目前仅支持基于 Token 的加密
	msgEncryptPlan := environ.GetString("BROKER_MESSAGE_ENCRYPT_OPTION", "TOKEN")

	conf.EdgeHttpPort = environ.GetString("EDGE_LISTEN_PORT", "7280")
	conf.EdgeEnabled = environ.GetBool("EDGE_ENABLED", false)

	zapConf := &zaplog.Config{
		Filename:   conf.AppName,
		Level:      zaplog.WARNING,
		Rotation:   10,
		Retention:  5,
		MaxBackups: 10,
		Compress:   false,
	}

	if conf.Debug {
		zapConf.Level = zaplog.DEBUG
	}

	handler := mq.New(conf)
	handler.SetLogger(zaplog.NewLogger(zapConf).Sugar())
	if msgEncrypt { // 设置消息加密
		handler.SetCryptoPlan(msgEncryptPlan)
	}

	handler.Serve()
}

```

### TCP-Producer

源码位于`micromq/sdk/producer.go`

- 关键在于实现`sdk.PHandler`接口

```go
package monitor

import (
	"github.com/Chendemo12/micromq/sdk"
	"github.com/Chendemo12/fastapi-tool/logger"
)

type ProducerHandler struct {
	sdk.PHandler
}

func run() {
	const MessageEncrypt = true
	const MessageEncryptPlan = "TOKEN"

	var logger logger.Iface

	prod := sdk.NewProducer(sdk.Config{
		Host:   "Host",
		Port:   "Port",
		Ack:    sdk.AllConfirm,
		Logger: logger,
		Token:  "",
	}, &ProducerHandler{})

	// 设置加密
	if MessageEncrypt {
		prod.SetCryptoPlan(MessageEncryptPlan)
	}

	// 连接broker
	err := prod.Start()
	if err != nil {
		panic(err)
	}

	// send
	form := map[string]any{"name": "micromq"}

	err = prod.Send(func(record *sdk.ProducerMessage) error {
		record.Topic = "TOPIC"
		record.Key = "KEY"

		return record.BindFromJSON(form)
	})

	if err != nil {
		logger.Error("report publisher failed: ", err.Error())
	} else {
		logger.Info("report publisher.")
	}
}


```

### TCP-Consumer

源码位于`micromq/sdk/consumer.go`

- 关键在于实现`sdk.CHandler`接口

```go

package monitor

import (
	"github.com/Chendemo12/micromq/sdk"
)

type ConsumerHandler struct {
	sdk.CHandler
}

func (c ConsumerHandler) Topics() []string {
	return []string{"TOPIC"}
}

func (c ConsumerHandler) Handler(record *sdk.ConsumerMessage) {
	// 处理接收到的消息
}

func run() {
	const MessageEncrypt = true
	const MessageEncryptPlan = "TOKEN"

	logger := logger.NewDefaultLogger()

	con, err := sdk.NewConsumer(sdk.Config{
		Host:   "Host",
		Port:   "Port",
		Ack:    sdk.AllConfirm,
		Logger: logger,
		Token:  "",
	}, &ConsumerHandler{})

	// 设置加密
	if MessageEncrypt {
		con.SetCryptoPlan(MessageEncryptPlan)
	}

	err = con.Start()
	if err != nil {
		panic(err)
	}
}
```

### edge

源码位于`micromq/sdk/httpr.go`

```go

package main

import "github.com/Chendemo12/micromq/sdk"

func run() {
	p := sdk.NewHttpProducer("127.0.0.1", "7072")
	p.SetToken(p.CreateSHA("token"))

	form := map[string]any{"name": "micromq"}
	resp, err := p.Post("topic", "key", form)

	if err != nil {
		println(err)
	}

	if resp.IsOK() {
		// send ok
	} else {
		println(resp.Error())
	}
}

```

- 路由：`/api/edge/product`
- HTTP 表单 `application/json`

```json

{
  "topic": "topic",
  "key": "key",
  "value": "value",
  "token": "token"
}

```

- openapi 文档

```json

{
  "info": {
    "title": "micromq",
    "version": "v0.3.5",
    "description": "micromq Api Service",
    "contact": {
      "name": "FastApi",
      "url": "github.com/Chendemo12/fastapi",
      "email": "chendemo12@gmail.com"
    },
    "license": {
      "name": "FastApi",
      "url": "github.com/Chendemo12/fastapi"
    }
  },
  "components": {
    "schemas": {
      "edge.ProducerForm": {
        "title": "ProducerForm",
        "type": "object",
        "description": "生产者消息投递表单, 不允许将多个消息编码成一个消息帧; \ntoken若为空则认为不加密; \nvalue是对加密后的消息体进行base64编码后的结果,依据token判断是否需要解密",
        "required": [],
        "properties": {
          "key": {
            "name": "key",
            "title": "Key",
            "type": "string",
            "required": false,
            "description": "消息键"
          },
          "value": {
            "name": "value",
            "title": "Value",
            "type": "string",
            "required": false,
            "description": "base64编码后的消息体"
          },
          "token": {
            "name": "token",
            "title": "Token",
            "type": "string",
            "required": false,
            "description": "认证密钥"
          },
          "topic": {
            "required": false,
            "description": "消息主题",
            "name": "topic",
            "title": "Topic",
            "type": "string"
          }
        }
      },
      "edge.ProductResponse": {
        "title": "ProductResponse",
        "type": "object",
        "description": "消息返回值; 仅当 status=Accepted 时才认为服务器接受了请求并正确的处理了消息",
        "required": [],
        "properties": {
          "message": {
            "title": "Message",
            "type": "string",
            "required": false,
            "description": "额外的消息描述",
            "name": "message"
          },
          "status": {
            "name": "status",
            "title": "Status",
            "type": "string",
            "required": false,
            "description": "消息接收状态",
            "enum": [
              "Accepted",
              "UnmarshalFailed",
              "TokenIncorrect",
              "Let-ReRegister",
              "Refused"
            ]
          },
          "offset": {
            "description": "消息偏移量",
            "name": "offset",
            "title": "Offset",
            "type": "integer",
            "required": false
          },
          "response_time": {
            "description": "服务端返回消息时的时间戳",
            "name": "response_time",
            "title": "ResponseTime",
            "type": "integer",
            "required": false
          }
        }
      },
      "fastapi.ValidationError": {
        "title": "ValidationError",
        "type": "object",
        "properties": {
          "loc": {
            "title": "Location",
            "type": "array",
            "items": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "integer"
                }
              ]
            }
          },
          "msg": {
            "title": "Message",
            "type": "string"
          },
          "type": {
            "title": "Error Type",
            "type": "string"
          }
        },
        "required": [
          "loc",
          "msg",
          "type"
        ]
      },
      "fastapi.HTTPValidationError": {
        "title": "HTTPValidationError",
        "type": "object",
        "required": [
          "detail"
        ],
        "properties": {
          "detail": {
            "title": "Detail",
            "type": "array",
            "items": {
              "$ref": "#/components/schemas/fastapi.ValidationError"
            }
          }
        }
      }
    }
  },
  "paths": {
    "/api/edge/product/async": {
      "post": {
        "responses": {
          "200": {
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/edge.ProductResponse"
                }
              }
            },
            "description": "OK"
          },
          "422": {
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/fastapi.ValidationError"
                }
              }
            },
            "description": "Unprocessable Entity"
          }
        },
        "tags": [
          "EDGE"
        ],
        "summary": "异步发送一个生产者消息",
        "description": "非阻塞式发送生产者消息，服务端会在消息解析成功后立刻返回结果，不保证消息已发送给消费者",
        "requestBody": {
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/edge.ProducerForm"
              }
            }
          },
          "required": true
        }
      }
    },
    "/api/edge/product": {
      "post": {
        "responses": {
          "200": {
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/edge.ProductResponse"
                }
              }
            },
            "description": "OK"
          },
          "422": {
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/fastapi.ValidationError"
                }
              }
            },
            "description": "Unprocessable Entity"
          }
        },
        "tags": [
          "EDGE"
        ],
        "summary": "发送一个生产者消息",
        "description": "阻塞式发送生产者消息，此接口会在消息成功发送给消费者后返回",
        "requestBody": {
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/edge.ProducerForm"
              }
            }
          },
          "required": true
        }
      }
    }
  },
  "openapi": "3.1.0"
}
```