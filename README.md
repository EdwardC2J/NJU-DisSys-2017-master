# NJU-DisSys-2017
This is the resource repository for the course Distributed System, Fall 2017, CS@NJU.

In Assignment 2 and Assignment 3, you should primarily focus on /src/raft/...

参考地址: https://github.com/bysui/mit6.824

该地址源代码实现Lab3 partA部分会出现map并发读写错误，需要在kvraft/server.go 第127行delete(kv.pendingOps, op_idx) 前后添加kv.mu.Lock()，kv.mu.Unlock()解锁操作

相关疑问，欢迎咨询 njuzengc@foxmail.com
