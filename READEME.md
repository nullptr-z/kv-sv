1. 像 KV server 这样需要高性能的场景，通信应该优先考虑 TCP 协议。所以我们暂时只支持 TCP，未来可以根据需要支持更多的协议，如 HTTP2/gRPC。还有，未来可能对安全性有额外的要求，所以我们要保证 TLS 这样的安全协议可以即插即用。总之，网络层需要灵活。
2. 应用层协议我们可以用 protobuf 定义。protobuf 直接解决了协议的定义以及如何序列化和反序列化。Redis 的 RESP 固然不错，但它的短板也显而易见，命令需要额外的解析，而且大量的 \r\n 来分隔命令或者数据，也有些浪费带宽。使用 JSON 的话更加浪费带宽，且 JSON 的解析效率不高，尤其是数据量很大的时候。protobuf 就很适合 KV server 这样的场景，灵活、可向后兼容式升级、解析效率很高、生成的二进制非常省带宽，唯一的缺点是需要额外的工具 protoc 来编译成不同的语言。虽然 protobuf 是首选，但也许未来为了和 Redis 客户端互通，还是要支持 RESP。
3. 服务器支持的命令我们可以参考 Redis 的命令集。第一版先来支持 HXXX 命令，比如 HSET、HMSET、HGET、HMGET 等。从命令到命令的响应，可以做个 trait 来抽象。
4. 处理流程中计划加这些 hook：收到客户端的命令后 OnRequestReceived、处理完客户端的命令后 OnRequestExecuted、发送响应之前 BeforeResponseSend、发送响应之后 AfterResponseSend。这样，处理过程中的主要步骤都有事件暴露出去，让我们的 KV server 可以非常灵活，方便调用者在初始化服务的时候注入额外的处理逻辑。
5. 存储必然需要足够灵活。可以对存储做个 trait 来抽象其基本的行为，一开始可以就只做 MemDB，未来肯定需要有支持持久化的存储。
6. 需要支持配置，但优先级不高。等基本流程搞定，使用过程中发现足够的痛点，就可以考虑配置文件如何处理了。

# core idea:

- 从 TCPStream 读取 Frame
- Frame 头部 Header 4 字节包换了数据字节流的长度
- 其中 Header 首字节位，标识数据流部分是否压缩过

## why

TcpStream：TCP 协议的报文，可以通过自定义封包/解析帧(Frame)，实现自己的上层协议，关于帧长度可以直接计算 buf length，帧类型可以使用 protobuf 来完成

## AsyncReadExt

`tokio::io::util::async_read_ext::AsyncReadExt` 有大量用作读取的异步方法

## 添加 Pub、Sub 操作

需要引入多路复用的协议，http2 太重了，

yamux: 双向流的多路复用协议，机制简单，类似 http 的多路复用

## 待完成

[] tokio-yamux 替代 yamux
[] 实现 stream 、Sink
