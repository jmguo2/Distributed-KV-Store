# Distributed-KV-Store

## How to use
Launch an instance with `python server.py {1, 2, 3...} {L, E}`
Make sure to launch instances in order of PID, starting with the smallest.

`put [key] [integer value]`
`get [key]`
`dump`
`delay [float ms]`
