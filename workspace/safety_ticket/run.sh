#!/bin/sh
# 启动配电检修安全票协同系统
cd "$(dirname "$0")"
exec python3 server.py
