# 阶段一：构建镜像
FROM golang:1.20 AS builder

WORKDIR /app

# 将源代码复制到容器中
COPY . .

# 编译可执行程序
RUN go build -o synshare-mq

# 阶段二：构建最终镜像
FROM alpine:latest

WORKDIR /app

# 复制编译后的可执行程序到最终镜像中
COPY --from=builder /app/synshare-mq .

# 设置环境变量
ENV DEBUG=1
ENV LISTEN_PORT=7270

# 暴露端口
EXPOSE 8080

# 运行可执行程序
CMD ["./synshare-mq"]
