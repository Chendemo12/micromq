#!/bin/bash

echo "building..."

go mod tidy
go mod vendor

export CGO_ENABLED=0 # 禁用CGO
# -s：忽略符号表和调试信息。
# -w：忽略DWARFv3调试信息，使用该选项后将无法使用gdb进行调试。
go build -tags=jsoniter -ldflags="-s -w" -gcflags='-l -l -l -m' ./main.go

mkdir ./bin
mv main ./bin/

cd ..
