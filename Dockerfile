# 阶段一：构建镜像
FROM golang:1.21 AS builder

WORKDIR /app
# 将源代码复制到容器中
COPY . .

# 编译可执行程序
ENV GOPROXY=https://proxy.golang.com.cn,direct
ENV CGO_ENABLED=0
RUN go mod download
RUN go mod vendor
RUN go build -ldflags="-s -w" -gcflags='-l -l -l -m' -o micromq


# 阶段二：构建最终镜像
FROM alpine:latest

# 指定镜像名称和版本号
LABEL maintainer="micromq-broker"

# 复制编译后的可执行程序到最终镜像中
COPY --from=builder /app/micromq .

ENV BROKER_EDGE_LISTEN_PORT=7271
ENV BROKER_CORE_LISTEN_PORT=7270

# 暴露端口
EXPOSE 7270
EXPOSE 7271

# 运行可执行程序
CMD ["./micromq"]
