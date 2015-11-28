---
layout: post
title: Hadoop源码分析之RPC Server1
category: Source
tags: BigData
keywords: 
description: 
---

![6-1 Server method](https://n4tfqg.blu.livefilestore.com/y2pbb8Y9FNbFjyCy5JvhWXslL8vW3NSk4IcG_dMddJZlnYHFuPi2H8AsXTp-y1d9AoIn8QN-00QD8kKd0n4uqgtyR8F_iU9yw71I92e-ymH5JTiKsnaIFbN_8_ch0U7E5Ln/6-1%20Server%20method.png?psid=1)  

###Java NIO
####Buffer
**容量Capacity**		缓冲区能够容纳的数据元素的最大数量. 这一容量在缓冲区创建时被设定, 并且永远不能被改变.  
**上界Limit**	 		缓冲区的第一个不能被读或写的元素. 或者说缓冲区中现存元素的计数.  
**位置Position** 		下一个要被读或写的元素的索引. 位置会自动由相应的get()和put()方法更新.  
position在调用put()时指出了下一个数据元素应该被插入的位置, 或者当get()被调用时指出下一个元素应从何处检索  

对缓冲区的一些操作的术语:  

    填充/ 写入/ fill/ put  
    释放/ 读取/ drain/ get  

![6-2 Java NIO put](https://n4tfqg.blu.livefilestore.com/y2p3E28Gc_30D1BnL8-rhS-VLG8AIm1LRrSNbq76z-YnaICPyXjuYApUGKo4JkTzy0EKJKeXrk2vyPFRBrMcsLPR3aExHDRAkl7VsyKraKsWzB3Xk3nGQCwzygkZLYsVSkY/6-2%20NIO%20Buffer%20put.png?psid=1)  
![6-2 Java NIO get](https://n4tfqg.blu.livefilestore.com/y2pPbq0Yes4ZB71KjJRzkdJa-ozIFgNETmwqzH8qETzCHyi5ATVgnb8fizcZi9h6Z-B6riGnTWkHr9gqKUFSGFSz6HcLUrT_kYWZyDiLdSzPXvpX1Vds-5M2GeJd4xP98Cc/6-2%20NIO%20Buffer%20get.png?psid=1)  

**init-put-flip-get-clear**  
我们已经写满了缓冲区(3右), 现在我们必须准备将其清空. 我们想把这个缓冲区传递给一个通道, 以使内容能被全部写出,或者缓冲区还没被写满(3左)就开始读取. 但如果通道现在在缓冲区上执行get(), 那么它将从我们刚刚插入的有用数据之外取出未定义数据. 如果我们将位置值重新设为0, 通道就会从正确位置开始获取, 但是它是怎样知道何时到达我们所插入数据末端的呢？这就是上界属性limit被引入的目的. 上界属性指明了缓冲区有效内容的末端. 我们需要将上界属性设置为当前位置, 然后将位置position重置为0.  
Buffer的flip()方法将一个能够继续添加数据元素的填充状态(fill)的缓冲区翻转成一个准备读出元素的释放状态(drained)  

```
	public static void testBufferPutAndGet() throws Exception{
		// 1. init
		ByteBuffer byteBuffer = ByteBuffer.allocate (100);
		printMsg(byteBuffer); //[pos=0 lim=100 cap=100]
		
		// 2. put
		byte[] bytes = new byte[]{(byte)'H',(byte)'e',(byte)'l',(byte)'l',(byte)'o',(byte)'W',(byte)'o',(byte)'r',(byte)'l',(byte)'d'};
		byteBuffer.put(bytes); 
		printMsg(byteBuffer); //[pos=10 lim=100 cap=100]
		
		// 3. flip
		byteBuffer.flip();
		printMsg(byteBuffer); //[pos=0 lim=10 cap=100]

		// 4. get
		while(byteBuffer.hasRemaining()){
			byte b = byteBuffer.get();
			printMsg(byteBuffer, b);
		}
		
		// 5. clear
		byteBuffer.clear();
		printMsg(byteBuffer); //[pos=0 lim=100 cap=100]
	}
```


打印信息  

```
java.nio.HeapByteBuffer[pos=0 lim=100 cap=100]		1. init
java.nio.HeapByteBuffer[pos=10 lim=100 cap=100]		2. put
java.nio.HeapByteBuffer[pos=0 lim=10 cap=100]		3. flip
java.nio.HeapByteBuffer[pos=1 lim=10 cap=100] H
java.nio.HeapByteBuffer[pos=2 lim=10 cap=100] e
java.nio.HeapByteBuffer[pos=3 lim=10 cap=100] l
java.nio.HeapByteBuffer[pos=4 lim=10 cap=100] l
java.nio.HeapByteBuffer[pos=5 lim=10 cap=100] o
java.nio.HeapByteBuffer[pos=6 lim=10 cap=100] W
java.nio.HeapByteBuffer[pos=7 lim=10 cap=100] o
java.nio.HeapByteBuffer[pos=8 lim=10 cap=100] r
java.nio.HeapByteBuffer[pos=9 lim=10 cap=100] l
java.nio.HeapByteBuffer[pos=10 lim=10 cap=100] d
java.nio.HeapByteBuffer[pos=0 lim=100 cap=100]		5. clear
```


####Channel
![6-3 Channel](https://n4tfqg.blu.livefilestore.com/y2pz3lj2-gjdYRl_kz4bqLJXxkwCLJl2oylPO9IeSKUB7irYKdRnjcM_IaoywKSqLOlOxlIPBtCWIvF4Tb2x3uPr2EaJNsX5176sjLGnxdOL5GmcJX0OZOxycV-cRsuLw_h/6-3%20NIO%20Channel%20read%20write.png?psid=1)  

```
	public static void testBufferToWriteChannel() throws Exception{
		// 1. init
		ByteBuffer byteBuffer = ByteBuffer.allocate (100);
		printMsg(byteBuffer); //[pos=0 lim=100 cap=100]
		
		// 2. put
		byte[] bytes = new byte[]{(byte)'H',(byte)'e',(byte)'l',(byte)'l',(byte)'o',(byte)'W',(byte)'o',(byte)'r',(byte)'l',(byte)'d'};
		byteBuffer.put(bytes); 
		printMsg(byteBuffer); //[pos=10 lim=100 cap=100]
		
		// 3. flip
		byteBuffer.flip();
		printMsg(byteBuffer); //[pos=0 lim=10 cap=100]
		
		// 4. get [->WriteChannel]
		WritableByteChannel dest = Channels.newChannel (System.out);
		dest.write(byteBuffer);
		printMsg(byteBuffer); //[pos=10 lim=10 cap=100]
		
		// 5. clear
		byteBuffer.clear();
		printMsg(byteBuffer); //[pos=0 lim=100 cap=100]
	}
	
	public static void testReadChannelToWriteChannel() throws Exception{
		// 1. init
		ByteBuffer byteBuffer = ByteBuffer.allocate (100);
		
		// 2. put [<-ReadChannel]
		ReadableByteChannel source = Channels.newChannel (System.in);
		source.read(byteBuffer); // 输入HelloYOU八个字节，按回车\n,又两个字节,共十个字节.和上面的测试方法一样
		printMsg(byteBuffer);
		
		// 3. flip
		byteBuffer.flip();
		printMsg(byteBuffer);
		
		// 4. get [->WriteChannel]
		WritableByteChannel dest = Channels.newChannel (System.out);
		dest.write(byteBuffer);
		printMsg(byteBuffer);
		
		// 5. cliear
		byteBuffer.clear();
		printMsg(byteBuffer);
	}
```


####Socket(TD)

####Selector(TD) 


###工作原理
ipc.Server是抽象类, 抽象类不能实例化, 实例化的是ipc.Server抽象类的实现类, 即ipc.RPC.Server.  
Server唯一抽象的方法. Server提供了一个架子, Server的具体功能, 需要具体类来完成. 而具体类, 当然就是实现call方法.  

    public abstract Writable call(Class<?> protocol, Writable param, long receiveTime) throws IOException;  

**Server.Call  包含了一次请求**  
和Client.Call类似, Server.Call包含了一次请求, 其中id和param的含义和Client.Call是一致的. 不同点:  
connection是该Call来自的连接, 当请求处理结束时, 相应的结果会通过相同的connection, 发送给客户端.  
timestamp是请求到达的时间戳, 如果请求很长时间没被处理, 对应的连接会被关闭, 客户端也就知道出错了.  
response是请求处理的结果, 可能是一个Writable的串行化结果, 也可能一个异常的串行化结果. 

**Server.Connection 维护了一个来自客户端的socket连接.**  
它处理版本校验, 读取请求并把请求发送到请求处理线程(Handler), 接收处理结果并把结果(Responder)发送给客户端.  
Hadoop的Server采用了Java的NIO, 这样的话就不需要为每一个socket连接建立一个线程, 读取socket上的数据.  
在Server中, 只需要一个线程, 就可以accept新的连接请求和读取socket上的数据, 这个线程, 就是Listener.  

**Server.Handler 请求处理线程一般有多个.**  
Handler的run方法循环地取出一个Server.Call, 调用Server.call方法, 搜集结果并串行化, 然后将结果放入Responder队列中.  
对于处理完的请求, 需要将结果写回去, 同样, 利用NIO, 只需要一个线程, 相关的逻辑在Responder里.  



####Client, Server & SocketChannel, Socket
**Client.Connection**  

    Socket socket = socketFactory.createSocket();   	// connected socket 		
    InetSocketAddress server = remoteId.getAddress(); // 客户端要连接到的服务器地址
    NetUtils.connect(this.socket, server, 20000);		// 客户端连接服务器
Socket: 用于和服务器建立连接的Socket,并且利用Socket得到输出流用于给服务器发送数据, 得到输入流用于读取服务器发送的数据.  

客户端建立Socket连接(阻塞IO), 服务器建立ServerSocket连接采用ServerSocketChannel(NIO)  
**Server.Listener**  

    InetSocketAddress address = new InetSocketAddress(bindAddress, port); 	// Server地址
    ServerSocketChannel acceptChannel = ServerSocketChannel.open(); 		// the accept channel
    acceptChannel.configureBlocking(false); 								// 设置为非阻塞模型
　　	
    acceptChannel.socket().bind(address, backlogLength);					// 绑定Server地址到ServerSocket上
    acceptChannel.register(selector, SelectionKey.OP_ACCEPT);				// Register accepts on the server socket with the selector
服务器的监听器Listener.run轮询监听客户端的连接, 当有一个客户端连接(doAccept)就对应建立一个SocketChannel  

    ServerSocketChannel server = (ServerSocketChannel) key.channel();		// 根据SelectionKey获取发生在此事件的服务器连接通道
    SocketChannel channel = server.accept();								// 接受客户端的连接, 建立和客户端进行连接的SocketChannel
获得SocketChannel后首先往SocketChannel注册读事件, 然后新建对应的Server.Connection, 等待读取客户端的数据  
Connection包装了SocketChannel即服务器和客户端之间的连接通道. 也是客户端和服务器的连接Socket.  

    SelectionKey readKey = reader.registerChannel(channel);				// 在SocketChannel上注册读事件
    Connection c = new Connection(readKey, channel, System.currentTimeMillis());
    readKey.attach(c);		// 将Connection对象附加到SelectionKey上, 这样在发生读取时可以从SelectionKey中获取附加的Connection对象

由于读取的线程可能有多个, 所以Listener会委托给Reader线程进行处理(通过把Connection附加到SelectionKey上, Reader根据选择键能取到Connection)  
Listener只是负责监听客户端的连接并给SocketChannel注册读操作,  Connection才负责建立和客户端的连接,并采用该SocketChannel读取客户端数据  

**Server.Connection**  

    SocketChannel channel;		// 服务器接受客户端的连接,建立了一个SocketChannel通道
    Socket socket;				// 通过SocketChannel获得对应的Socket连接	 
    LinkedList<Call> responseQueue;// 维护一个响应队列: 当调用完成,将调用Call放入响应队列,让Responder取出Call处理回写 
Connection连接对象持有服务器和客户端之间的SocketChannel连接通道, 因此当有客户端发送数据时, Connection会开始读取客户端的数据	 
Connection负责服务器和客户端的连接, 所以当服务器有响应数据要写给客户端, Connection还应该负责响应的回写.保证数据能写到客户端.  
所以还要维护一个响应对象.但是具体的回写操作交给Responder对象. 正如具体的调用操作交给了Handler对象处理.  

    Call call = new Call(id, param, this);
    callQueue.put(call);			// 维护一个调用对象: 读取客户端数据,将调用放入调用队列,让handler取出Call处理调用  
读取客户端的数据后, 将客户端的调用封装成服务器的Call对象, 并且关联了this当前的Connection对象.  
这样响应数据处理就能根据call得到Connection,从而得到Connection的SocketChannel, 就能将call.response数据写入SocketChannel通道.  
Client就能根据Socket得到输入流来获取服务端写入的数据(阻塞读取输入流,并不是采用非阻塞方式从通道中读取).  

**Server.Responder**  

    SocketChannel channel = call.connection.channel;
    channelWrite(channel, call.response);

**Server.Call**  

    Connection connection; 		// connection to client 服务器和客户端的连接
    ByteBuffer response;       	// the response for this call 调用的响应数据缓冲



####Server.Call对象贯穿服务端的整个流程
**Listener**监听到客户端的连接,Connection建立和客户端的连接读取客户端数据.  

   Call会封装了当前服务器和客户端的连接.  
   并把Call放入调用队列让Handler处理  
**Handler**从调用队列中取出Call, 执行真正的调用操作:  

   把调用结果, 状态设置到Call的响应缓冲中  
   再把Call放入响应队列  
**Responder**从响应对象中取出Call, 执行真正的写入操作:  

   将call中的响应缓冲写入通道等待客户端读取  


responseQueue响应队列定义在Connection, 在Responder中操作(添加和获取)  
callQueue调用队列为全局属性, 在Connection中添加(有客户端写入即客户端调用), 在Handler中获取(反射调用)  


###Server
ipc.Server是服务端的抽象实现, 定义了一个抽象的IPC服务. IPC Server接收Client发送的参数值,并返回响应值.  
同时作为IPC模型的服务端, 它要维护Client端到Server端的一组连接.  

```
/** An abstract IPC service.  IPC calls take a single Writable as a parameter, and return a Writable as their value.
 * A service runs on a port and is defined by a parameter class and a value class. */
public abstract class Server {
  private String bindAddress; 			//服务端绑定的地址 
  private int port;                   	// port we listen on 服务端监听端口 
  private int handlerCount;            	// number of handler threads 处理线程的数量
 
  private int readThreads;             	// number of read threads 读取线程的数量
  private Class<? extends Writable> paramClass;   // class of call parameters 调用的参数的类, 必须实现Writable序列化接口 
  private int maxIdleTime;            	// the maximum idle time after which a client may be disconnected 当一个客户端断开连接后的最大空闲时间 
  private int thresholdIdleConnections; 	// the number of idle connections after which we will start cleaning up idle connections 可维护的最大连接数量 
  int maxConnectionsToNuke;        	// the max number of connections to nuke during a cleanup
  protected RpcInstrumentation rpcMetrics; 	//维护RPC统计数据 
  private Configuration conf;				// 配置类实例  
  private SecretManager<TokenIdentifier> secretManager;
  private int maxQueueSize;			// 处理器Handler实例的队列大小 
  private final int maxRespSize;
  private int socketSendBufferSize;		// Socket Buffer大小 
  private final boolean tcpNoDelay; 		// if T then disable Nagle's Algorithm

  volatile private boolean running = true;	// true while server runs  Server是否运行 
  private BlockingQueue<Call> callQueue;	// queued calls 维护调用实例的队列 
  //maintain a list of client connections  Server端维护Client端的连接列表, 注意对比Client的connections: 所有Client连接Server的列表
  private List<Connection> connectionList = Collections.synchronizedList(new LinkedList<Connection>());  
  private Listener listener = null;			//服务端监听器: 监听Server Socket的线程, 为处理器Handler线程创建任务 
  private Responder responder = null;	//服务端写回客户端的响应: 响应客户端RPC调用的线程, 向客户端调用发送响应信息 
  private int numConnections = 0;		//客户端的连接数量
  private Handler[] handlers = null;		//处理器Handler线程数组 
    
  /** Constructs a server listening on the named port and address.  
   * Parameters passed must be of the named class.  
   * The handlerCount determines the number of handler threads that will be used to process calls. */
  protected Server(String bindAddress, int port, Class<? extends Writable> paramClass, int handlerCount, 
      Configuration conf, String serverName, SecretManager<? extends TokenIdentifier> secretManager) {
    this.bindAddress = bindAddress;
    this.conf = conf;
    this.port = port;
    this.paramClass = paramClass;
    this.handlerCount = handlerCount;
    this.callQueue  = new LinkedBlockingQueue<Call>(maxQueueSize);
    // ... 初始化属性比如处理器数量, 读取线程数量等连接信息
    // Start the listener here and let it bind to the port
    listener = new Listener();
    this.port = listener.getAddress().getPort();    
    this.rpcMetrics = RpcInstrumentation.create(serverName, this.port);
    this.tcpNoDelay = conf.getBoolean("ipc.server.tcpnodelay", false);
    responder = new Responder(); // Create the responder here
    if (isSecurityEnabled)  SaslRpcServer.init(conf);
  }

  private void closeConnection(Connection connection) {
    synchronized (connectionList) {
      if (connectionList.remove(connection))
        numConnections--;
    }
    try {
      connection.close();
    } catch (IOException e) {}
  }

  // NameNode在获得Server后, 会调用server.start()启动服务端. 三个对象responder,listener,handlers都是线程类, 都调用start()
  /** Starts the service.  Must be called before any calls will be handled. */
  public synchronized void start() {
    responder.start();		// 启动调用的响应数据处理线程 
    listener.start();		// 启动监听线程  
    handlers = new Handler[handlerCount];  // 启动多个处理器线程 
    for (int i = 0; i < handlerCount; i++) {
      handlers[i] = new Handler(i);
      handlers[i].start();
    }
  }

  /** Stops the service.  No new calls will be handled after this is called. */
  public synchronized void stop() {
    running = false;
    if (handlers != null) {	// 先中断全部处理器线程 
      for (int i = 0; i < handlerCount; i++) {
        if (handlers[i] != null) {
          handlers[i].interrupt();
        }
      }
    }
    listener.interrupt();	// 终止监听器线程 
    listener.doStop();
    responder.interrupt();	// 终止响应数据处理线程 
    notifyAll();
    if (this.rpcMetrics != null) {
      this.rpcMetrics.shutdown();
    }
  }
}
```


Server的构造方法对一个Server实例进行初始化, 包括一些静态信息如绑定地址bindAddress+port、维护连接数量handlerCount、队列callQueue等, 还有一些用来处理Server端事务的线程:Listener(Listener中的Reader[]), Handler[], Responder等  

RPC.getServer会new一个RPC.Server, 在RPC.Server中会调用父类的构造器, 即调用ipc.Server抽象类的构造器, 完成初始化工作. 新建一个RPC Server的参数bindAddress, port的传入由RPC.getServer的调用者来传入. 注意第三个参数为Invocation.class. 则ipc.Server.paramClass=Invocation.class

![6-4 Client Server](https://n4tfqg.blu.livefilestore.com/y2payKljIj97qa6bI9ENa_-NCxD6kK2Cd5YoWgRic4L2e0PudQaHDsya7ETJv2-rz0J2GUWTOYgINWmyBsUk_ccKq0yROAfe_jPzrb0lOKkhMYOlLT0mAQ12Qq02MQR_UkT/6-4%20Client%20Server.png?psid=1)  

###Call
Server.Call内部类表示Server端使用队列维护的调用实体类  

```
  /** A call queued for handling. */
  private static class Call {
    private int id;                	// the client's call id 客户端的RPC调用对象Call的id =  Client.Call.id
    private Writable param;       	// the parameter passed 客户端的RPC调用对象Call的参数 = Client.Call.param
    private Connection connection;	// connection to client 到客户端的连接实例, 在服务端持有这个对象, 就能知道是哪个客户端连接
    private long timestamp;     	// the time received when response is null; the time served when response is not null 向客户端调用发送响应的时间戳 
    private ByteBuffer response;   // the response for this call 当前RPC调用的响应, 向客户端调用响应的字节缓冲区  => Client.Call.value
    
    public Call(int id, Writable param, Connection connection) { 
      this.id = id;
      this.param = param;
      this.connection = connection;
      this.timestamp = System.currentTimeMillis();
      this.response = null;
    }
    public void setResponse(ByteBuffer response) {
      this.response = response;
    }
  }
```


###Listener
Client端的底层通信采用了阻塞式IO编程, Server端采用Listener线程类监听客户端的连接, 并为Handler处理器线程创建处理任务  
Server.Listener主要负责两个阶段的任务  

  当服务器运行时, 不断地通过选择器来选择继续的通道, 处理基于该选择的通道上通信;  
  当服务器不再运行以后, 需要关闭通道、选择器、全部链接, 释放一切资源  
Listener的run方法会调用doAccept(), Listener.Reader的run方法会调用doRead().  doAccept()和doRead()都在Listener内.  

```
  private static final ThreadLocal<Server> SERVER = new ThreadLocal<Server>();

  /** Returns the server instance called under or null.  May be called under #call(Writable, long)implementations, 
   * and under Writable methods of paramters and return values.  Permits applications to access the server context.*/
  public static Server get() {
    return SERVER.get();
  }

  //maintain a list of client connections 维护客户端的连接列表, 这里的Connection是Server.Connection
  private List<Connection> connectionList = Collections.synchronizedList(new LinkedList<Connection>());

  /** Listens on the socket. Creates jobs for the handler threads 监听客户端Socket连接, 为handler线程创建任务 */
  private class Listener extends Thread {
    private ServerSocketChannel acceptChannel = null; 	//the accept channel 服务端通道
    private Selector selector = null; 					//the selector that we use for the server 选择器(NIO)
    private Reader[] readers = null;
    private int currentReader = 0;
    private InetSocketAddress address; 				//the address we bind at 服务端地址
    private Random rand = new Random();
    private long lastCleanupRunTime = 0; 			//the last time when a cleanup connection (for idle connections) ran
    private long cleanupInterval = 10000; 			//the minimum interval between two cleanup runs
    private int backlogLength = conf.getInt("ipc.server.listen.queue.size", 128);
    private ExecutorService readPool; 				//读取池, 任务执行服务(并发)
   
    public Listener() throws IOException {
      address = new InetSocketAddress(bindAddress, port);
      acceptChannel = ServerSocketChannel.open(); 			// Create a new server socket 创建服务端Socket通道
      acceptChannel.configureBlocking(false);  				// and set to non blocking mode设置为非阻塞模式

      bind(acceptChannel.socket(), address, backlogLength); // Bind the server socket to the local host and port将ServerSocket绑定到本地端口
      port = acceptChannel.socket().getLocalPort();  		// Could be an ephemeral port
      selector= Selector.open();  							// create a selector 创建一个监听器的Selector
      readers = new Reader[readThreads];					//读取线程数组
      readPool = Executors.newFixedThreadPool(readThreads);	//启动多个reader线程,为了防止请求多时服务端响应延时的问题 
      for (int i = 0; i < readThreads; i++) {
        Selector readSelector = Selector.open();			//每个读取线程都创建一个Selector
        Reader reader = new Reader(readSelector);
        readers[i] = reader;
        readPool.execute(reader);
      }
      // 向通道acceptChannel注册上述selector选择器, 选择器的键为Server Socket接受的操作集合 
      acceptChannel.register(selector, SelectionKey.OP_ACCEPT); // ① Register accepts on the server socket with the selector. 注册连接事件 
      this.setName("IPC Server listener on " + port);
      this.setDaemon(true);
    } 
    
    // 在启动Listener线程时listener.start(), 服务端会一直等待客户端的连接
    public void run() {
      SERVER.set(Server.this); //使用ThreadLocal本地线程,设置当前监听线程本地变量的拷贝 
      while (running) {
        SelectionKey key = null;
        try {
          selector.select();	//选择一组key集合, 这些选择的key相关联的通道已经为I/O操作做好准备 
          Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
          while (iter.hasNext()) {
            key = iter.next();
            iter.remove();
            if (key.isValid()) {
              if (key.isAcceptable())	//如果该key对应的通道已经准备好接收新的Socket连接 
                doAccept(key);		// ② 建立连接,服务端接受客户端连接
            }
            key = null;
          }
        } catch (Exception e) {
          closeCurrentConnection(key, e);
        }
        cleanupConnections(false);
      }
  	  // 跳出while循环即running=false. 服务器已经不再运行, 监听器不再监听客户端的连接. 需要关闭通道、选择器、全部的连接对象 
      synchronized (this) {
        acceptChannel.close();
        selector.close();
        selector= null;
        acceptChannel= null;
        while (!connectionList.isEmpty()) { // clean up all connections
          closeConnection(connectionList.remove(0));
        }
      }
    }
    
    //根据key关联的Server Socket通道, 接收该通道上Client端到来的连接 
    void doAccept(SelectionKey key) throws IOException, OutOfMemoryError { // ②
      Connection c = null;
      ServerSocketChannel server = (ServerSocketChannel) key.channel();	//获取到Server Socket 通道 
      SocketChannel channel;
      while ((channel = server.accept()) != null) {						//建立连接server.accept()
        channel.configureBlocking(false);								//设置SocketChannel通道为非阻塞
        channel.socket().setTcpNoDelay(tcpNoDelay);						//设置TCP连接是否延迟 
        Reader reader = getReader(); 									//从readers池中获得一个reader 
        try {
          reader.startAdd();											//激活Reader.readSelector, 设置adding为true 
          SelectionKey readKey = reader.registerChannel(channel); 		// ③ 将读事件设置成兴趣事件 
          c = new Connection(readKey, channel, System.currentTimeMillis());	//创建一个连接对象 
          readKey.attach(c);											//将connection对象注入readKey 
          synchronized (connectionList) {
            connectionList.add(numConnections, c);						//加入Server端连接维护列表 
            numConnections++;											//连接数+1
          }        
        } finally {
          reader.finishAdd();  //设置adding为false,采用notify()唤醒一个reader, 初始化Listener时启动的每个reader都使用了wait()方法等待
        } // 当reader被唤醒, reader会执行doRead()
      }
    }

    void doRead(SelectionKey key) throws InterruptedException {  // ④
      int count = 0;
      Connection c = (Connection)key.attachment();
      if (c == null) {
        return;  
      }
      c.setLastContact(System.currentTimeMillis());
      
      try {
        count = c.readAndProcess();
      } catch (InterruptedException ieo) { throw ieo;
      } catch (Exception e) { count = -1; } //so that the (count < 0) block is executed
      if (count < 0) {
        closeConnection(c);
        c = null;
      } else { c.setLastContact(System.currentTimeMillis()); }
    }   

    synchronized void doStop() {
      if (selector != null) {
        selector.wakeup();
        Thread.yield();
      }
      if (acceptChannel != null) {
        try {
          acceptChannel.socket().close();
        } catch (IOException e) {}
      }
      readPool.shutdown();
    }

    // The method that will return the next reader to work with Simplistic implementation of round robin for now
    Reader getReader() {
      currentReader = (currentReader + 1) % readers.length;
      return readers[currentReader];
    }
  }
```


####Listener.Reader
当Client创建Socket连接, 连接到Server时, Server端的Listener的run方法负责监听客户端的连接, 通过doAccept()接受客户端的连接: 为客户端的连接通道SocketChannel注册读事件OP_READ, 同时给选择键附加Connection对象.  当Client的连接被Server接受后, Client开始向Server发送数据请求(RPC调用), Server.Listener.Reader负责读取客户端的请求数据, Listener本身并不负责读取客户端的请求数据而是交由Reader线程去做. Listener的Reader线程有多个,Reader的run方法通过doRead()从通道的选择键获取附件的Connection对象(在doAccept这一步向Socket通道注册了读事件, 并附件了Connection对象, 所以当有读取事件发生时能从选择键中获取附加的这个对象), 由Connection对象读取和处理数据.  

```
    private class Reader implements Runnable {
      private volatile boolean adding = false; //读取线程是否正在添加中,如果是,等待一秒钟
      private Selector readSelector = null; //读取线程的Selector选择器

      Reader(Selector readSelector) {
        this.readSelector = readSelector;
      }
      public void run() {
        synchronized (this) {
          while (running) {
            SelectionKey key = null;
            readSelector.select();
            while (adding) { // 在Listener.run线程接受客户端连接的doAccept方法中, 要构造Connection,并附加到SocketChannel的选择键上
              this.wait(1000); // 如果这个过程还没完成,说明Connection对象还没构造完成, Reader.run线程还不能读取客户端发送过来的请求数据
            }              
 			// Connection对象构造完毕, Reader线程可以开始读取客户端发过来的数据了
            Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
            while (iter.hasNext()) {
              key = iter.next();
              iter.remove();
              if (key.isValid()) {
                if (key.isReadable()) {
                  doRead(key);   // ④
                }
              }
              key = null;
            }
          }
        }
      }

      /** This gets reader into the state that waits for the new channel to be registered with readSelector.
       * If it was waiting in select() the thread will be woken up, otherwise whenever select() is called
       * it will return even if there is nothing to read and wait  in while(adding) for finishAdd call*/
      public void startAdd() 
       	adding = true;
        readSelector.wakeup();
      }
      public synchronized void finishAdd() {
        adding = false;
        this.notify();        
      }

      public synchronized SelectionKey registerChannel(SocketChannel channel) {
          return channel.register(readSelector, SelectionKey.OP_READ);  // ③
      }
    }
```



####NIO通信流程
① 初始化服务器时,创建监听器, 在监听器的构造方法里会创建ServerSocketChannel, 选择器, 以及多个读取线程. 并在服务端通道上注册OP_ACCEPT操作  

    acceptChannel.register(selector, SelectionKey.OP_ACCEPT);  

启动服务器会调用listener.start(), listener是个线程类, 会调用run().  
监听器会一直监听客户端的请求, 通过监听器的Selector选择器进行轮询是否有感兴趣的事件发生(服务器感兴趣的是上面注册的接受连接事件)  

② 当客户端连接服务端, 被Selector捕获到该事件, 因为在ServerSocketChannle对OP_ACCEPT操作感兴趣,所以服务端接受了客户端的连接请求.  

    doAccept(SelectionKey key)

客户端连接到服务器, 服务端接受连接, 监听器会从读取线程池中选择一个读取线程, 委托给读取线程处理, 而不是监听器自己来处理.  

建立SocketChannel连接, 注意不是ServerSocketChannel. (ServerSocketChannle在整个通信过程中只建立一次即服务端启动的时候)  

    SocketChannel channel = server.accept();

③ 往建立的SocketChannel通道注册感兴趣的OP_READ操作. 此时接收读取事件的选择器不再是监听器的, 而是读取线程的选择器  

    SelectionKey readKey = reader.registerChannel(channel);
    channel.register(readSelector, SelectionKey.OP_READ);  //往readSelector注册感兴趣的OP_READ操作.读取线程的选择器负责轮询监听客户端的数据写入

同时根据(readKey, SocketChannel, 当前时间)建立一个Connection附加到readKey中.  
这个Connection对象是客户端和服务器的连接对象, 客户端和服务器建立连接后, 在后续的客户端写入数据过程也应该使用同一个Connection  

    Connection c = new Connection(readKey, channel, System.currentTimeMillis());
    readKey.attach(c);  // 在通信过程中如果想要保存某个对象,附加在selectionKey中
注册读操作后,服务端的监听器的读取线程就能读取客户端传入的数据  

④ 客户端开始向服务端写入数据, 读取线程Reader的选择器捕获到客户端的写入事件,  
因为读取线程注册了感兴趣的OP_READ操作,所以能够读取客户端的写入数据.  

    doRead(SelectionKey key)

读取事件的操作会根据selectionKey获得Connection, 这个Connection对象正是客户端和服务器建立连接时注入到readKey中的Connection对象  

    Connection c = (Connection)key.attachment();
具体的读取客户端的数据的操作就在该Connection的readAndProcess方法里  

    connection.readAndProcess();


**Listener.doAccept()接受连接过程**  
从readers池中获得一个reader线程  
reader.startAdd(); 激活readSelector, 设置adding为true --> 读线程监听客户端的数据写入,如果adding=true,表示Reader正在添加,再等待一秒钟  

  1. 将读事件设置成兴趣事件  
  2. 创建一个连接对象Connection  
  3. 将Connection附加到读事件的选择键上  
reader.finishAdd(); 设置adding为false,采用notify()唤醒一个reader, 初始化Listener时启动的每个reader都使用了wait()方法等待  
上面的三个步骤包装在设置Reader的adding属性以及使用notify()通知调用者两者之间. 是为了确保读取线程发生在设置读事件为感兴趣事件之后.  

基于NIO的事件模型采用选择器来轮询感兴趣的事件.只要有感兴趣的操作, 选择器就会捕获进行处理. 如果没有感兴趣的事件发生则没有操作.  
所以服务端接受客户端的连接和读取客户端的数据这两个操作过程发生的时刻完全是随机的.  
也就是说监听客户端连接的选择器和多个读取客户端数据的读取线程的选择器捕获事件也都是随机的.  

但是读取客户端的数据必须保证发生在客户端连接服务器之后.  
因为如果客户端没有连接服务器, 也就不会注册读取事件OP_READ到读取线程上. 因为注册OP_READ发生在在doAccept()客户端连接服务器操作中.  

初始化Listener时启动的每个Reader, 都会新建对应的选择器. Reader的默认字段adding=false  

    Selector readSelector = Selector.open();	  //每个读取线程都创建一个Selector
    Reader reader = new Reader(readSelector);
初始化时尽管adding=false在run()中不会执行this.wait(1000)的等待操作, 但是因为还没有客户端连接注册OP_READ事件所以选择器不会捕获该事件.  

客户端连接服务器,服务器接受连接,在doAccept()中, 注册OP_READ到读取线程的感兴趣事件  
1. 之前: 	设置adding=true并激活读取线程的选择器, 注意此时读取线程的选择器进行轮询操作是不会捕获到读取事件的,因为还没注册OP_READ事件  
  		所以读取线程的run()如果判断adding=true, 就知道选择器关注的SocketChannel上的OP_READ事件还没注册好,需要每隔一秒钟再判断  
2. 往建立的SocketChannel注册好OP_READ事件   
3. 之后:	设置adding=false并通知Reader读取线程不需要再等待下去, run()方法判断adding=false, 选择器开始轮询等待客户端的写入  



**Listener和Reader的选择器**  
Listener的选择器只有一个Selector selector, Listener有多个Reader, 每个Reader都有自己的选择器Selector readSelector.  
其实从类中定义的成员就可以看出来, Listener有成员变量Selector selector和Reader[] readers. 在new Listener时每个Reader都会新建对应的readSelector,  
Reader的类中有成员变量Selector readSelector. 所以从面向对象的角度来看, 在分析类中属性的一还是多的关系时, 查看属性是最直接的最明显的.  
 
Listener的选择器来监听客户端的连接, 当监听到有一个客户端连接服务器, 就会选取一个Reader, 并往Reader的选择器注册读取操作.  
这样具体的读取操作就交给了Reader进行处理. 因为Reader有多个, 所以如果有多个客户端连接并写入数据给服务器, 就可以开多个Reader同时读取.  



###小结
前面在Listener的doAccept中根据客户端的连接请求创建了一个Connection对象. 第一个参数SelectionKey并无多大用处.  

    new Connection(readKey, channel, System.currentTimeMillis());
Reader线程读取客户端发送的请求数据, 但是Reader也不进行具体的数据处理, 而是交给Connection对象处理:  

    Connection.readAndProcess();
由于Client向Server发起的连接请求(Listener.doAccept),以及Client向Server发送的数据请求(RPC调用, Reader.doRead)都要保证是同一个Connection,  
所以在Server的接受连接请求阶段需要将Connection附加到选择键上, 这样Server在接收数据请求时能取到同一个Connection.  

　　Listener:		接受客户端连接请求, 监听器线程  
　　Reader:		接收客户端数据请求, 读取线程  
　　Connection:	处理客户端数据请求, 读取和处理  

在分析Listener和Reader的代码时, 我们看到Client向Server发送的数据(writeRpcHeader, writeHeader, sendParam), 在Server端都还没有对应的处理.  
实际进行处理的是Connection类: 表示服务端一个连接的抽象, 主要是读取从Client发送的调用, 并把读取到的调用Client.Call实例加入到待处理的队列.  
对应的处理方法是readAndProcess()方法:  

　　读取远程过程调用的数据, 从一个Server.Connection的Socket通道中读取数据,  
　　并将调用任务加入到callQueue, 转交给Handler线程去处理  
  
