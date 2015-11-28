---
layout: post
title: Hadoop源码分析之HadoopRPC
category: Source
tags: BigData
keywords: 
description: 
---

![RPC method](https://n4tfqg.blu.livefilestore.com/y2pjEh1b2ZALqXC_jTvQq3Qt3sFJz3KJSx1LIgH9_ZT7-cX60nA92cx8jzqaqjvepFfTImVGULA6DwdrhpP9CjBxFBDjLXnnZaHAlyJWfbrnmLXfxBpRJBgXDqkDGed5J0a/4-1%20RPC%20method.png?psid=1)  
Hadoop的RPC中, Invoker实现了InvocationHandler的invoke方法.  Invoker会把所有跟这次调用相关的调用方法名, 参数类型列表, 参数列表打包:   封装成Invocation(封装了一个远程调用的所有相关信息, 实现序列化), 然后利用Client, 通过socket传递到服务器端.   
就是说, 在proxy类上的任何调用, 都通过Client发送到远程服务器上.   

RPC.Server实现了org.apache.hadoop.ipc.Server, 你可以把一个对象, 通过RPC, 升级成为一个服务器??.  
服务器接收到请求, 接收到的是Invocation对象, 反序列化后, 得到方法名, 方法参数列表和参数列表.  
利用Java反射, 可以调用对应的对象的方法. 调用的结果再通过socket返回给客户端, 客户端把结果解包后就可以返回给Dynamic Proxy的使用者了.  

![Hadoop动态代理](https://n4tfqg.blu.livefilestore.com/y2pQZ6_2VYqMCi-lQn2V2A2yxrnSMQOOC4xpDU1dXwA6vXAT9O5vk_BOaY3izJvmmCAkfSsD5BkapkrSnD0tC84JnKEacydUQ-nZFD882av83_geVxe07iWt4VkSf8I9fkU/4-2%20HadoopRPC%E7%9A%84%E5%8A%A8%E6%80%81%E4%BB%A3%E7%90%86.png?psid=1)  

下面进入RPC的源码分析, 以RPC中定义的顺序依次分析  
###Invocation
作为客户端和服务器之间的传输介质,实现序列化, 包含了客户端要调用的方法名,参数类型,参数值.

```
  /** A method invocation, including the method name and its parameters.*/
  private static class Invocation implements Writable, Configurable { //实现hadoop的序列化接口Writable,因为要在Client和Server之间传输该对象
    private String methodName;  		// The name of the method invoked.方法名 
    private Class[] parameterClasses;  	// The parameter classes. 参数类型集合 
    private Object[] parameters;  		// The parameter instances. 参数值 
    private Configuration conf;			// 配置文件实例, 用于序列化和反序列化ObjectWritable类型的参数类型和参数值

    public Invocation() {}
    public Invocation(Method method, Object[] parameters) {
      this.methodName = method.getName();
      this.parameterClasses = method.getParameterTypes();
      this.parameters = parameters;
    }
    // 序列化
    public void readFields(DataInput in) throws IOException {
      methodName = UTF8.readString(in); // 读取方法名 
      parameters = new Object[in.readInt()]; // 读取调用方法的参数值 
      parameterClasses = new Class[parameters.length]; // 参数类型 
      ObjectWritable objectWritable = new ObjectWritable();
      for (int i = 0; i < parameters.length; i++) { //数组类型,每个数组元素也都需要序列化
        parameters[i] = ObjectWritable.readObject(in, objectWritable, this.conf); // 读取每个调用参数值 
        parameterClasses[i] = objectWritable.getDeclaredClass(); // 读取每个参数类型 
      }
    }
    // 反序列化
    public void write(DataOutput out) throws IOException {
      UTF8.writeString(out, methodName); //向输出流out中写入方法名 
      out.writeInt(parameterClasses.length); //写入方法参数类型个数 
      for (int i = 0; i < parameterClasses.length; i++) {
        ObjectWritable.writeObject(out, parameters[i], parameterClasses[i], conf);  //写入方法参数
      }
    } 
  }
```


![Invocation序列化](https://n4tfqg.blu.livefilestore.com/y2p4gJdamEnAr_JW3CU2SXem2pD1YTiYe0qnQDFNMeBuSusIcnVCBEze1EeDDfWqaxIXRhN2MX1Aw1edbpF7n217kKFCQZywGSgVZ6YJIHgHYrKIRX92GNAOhhB_wr9arBv/4-3%20Invocation%E5%BA%8F%E5%88%97%E5%8C%96.png?psid=1)  

###ClientCache
定义了一个缓存Map. 通过客户端org.apache.hadoop.ipc.Client的SocketFactory可以快速取出对应的Client实例.   

```
  /* Cache a client using its socket factory as the hash key */
  static private class ClientCache {
    private Map<SocketFactory, Client> clients = new HashMap<SocketFactory, Client>();

    /** 从缓存Map中取出一个IPC Client实例, 如果缓存够中不存在, 就创建一个并加入到缓存Map中 
     * Construct & cache an IPC client with the user-provided SocketFactory if no cached client exists.
     * @param conf Configuration 配置信息仅用于超时, 客户端有连接池
     * @return an IPC client */
    private synchronized Client getClient(Configuration conf, SocketFactory factory) {
      // Construct & cache client.  The configuration is only used for timeout, and Clients have connection pools. 构造和缓存客户端
      // So we can either (a) lose some connection pooling and leak sockets, or (b) use the same timeout for all configurations.
      // Since the IPC is usually intended globally, not per-job, we choose (a). 因为IPC调用是全局的,而不是每个任务/线程?
      Client client = clients.get(factory);
      if (client == null) {
        client = new Client(ObjectWritable.class, conf, factory); // 通过反射实例化一个ObjectWritable对象, 构造Client实例 
        clients.put(factory, client);
      } else {
        client.incCount();  // 增加客户端client实例的引用计数 
      }
      return client;
    }
    /** Construct & cache an IPC client with the default SocketFactory if no cached client exists. */
    private synchronized Client getClient(Configuration conf) {
      return getClient(conf, SocketFactory.getDefault());
    }

    /** Stop a RPC client connection. A RPC client is closed only when its reference count becomes zero. 终止一个RPC客户端连接 */
    private void stopClient(Client client) {
      synchronized (this) {
        client.decCount(); // 该client实例的引用计数减1  
        if (client.isZeroReference()) { // 如果client实例的引用计数此时为0 
          clients.remove(client.getSocketFactory()); // 从缓存中删除 
        }
      }
      if (client.isZeroReference()) {  // 如果client实例引用计数为0, 需要关闭 
        client.stop(); // 停止所有与该client实例相关的线程 
      }
    }
  }
  private static ClientCache CLIENTS=new ClientCache();

  static Client getClient(Configuration conf) { //for unit testing only
    return CLIENTS.getClient(conf);
  }
```


####Client.refCount

```
  private int refCount = 1;
  private AtomicBoolean running = new AtomicBoolean(true); // if client runs
  private Hashtable<ConnectionId, Connection> connections = new Hashtable<ConnectionId, Connection>();

  synchronized void incCount() { /** Increment this client's reference count */
    refCount++;
  }
  synchronized void decCount() { /** Decrement this client's reference count */
    refCount--;
  }
  synchronized boolean isZeroReference() { /** Return if this client has no reference */
    return refCount==0;
  }

  public void stop() { /** Stop all threads related to this client.  No further calls may be made using this client. */
    if (!running.compareAndSet(true, false)) {
      return;
    }
    synchronized (connections) {
      for (Connection conn : connections.values()) { // wake up all connections
        conn.interrupt();
       }
    }
    while (!connections.isEmpty()) { // wait until all connections are closed
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
    }
  }
```


从前面的分析知道, 客户端通过getProxy()获得协议接口的代理类proxy: proxy在Client端,实现和协议相同的接口, proxy会(多次)调用协议接口的方法.  
并不是一次调用了就结束了. 每一次调用都需要首先获得客户端对象, 这样在调用协议接口的方法时, 转到调用句柄的invoke方法时, 客户端通过IPC调用到Server的具体实现方法.  
对于同一个客户端而言, 每一次调用可以共用这个客户端, 所以引入了Map缓存ClientCache类.  
对客户端的引用计数+1是发生在getProxy()的调用过程中即生成proxy代理类, 注意: 同一个proxy多次调用协议接口时对客户端的引用计数并不会变化.  


###Invoker
实现了java.lang.reflect.InvocationHandler接口, 是一个代理实例的调用句柄实现类  

```
  private static class Invoker implements InvocationHandler {
    private Client.ConnectionId remoteId; 	// 远程服务器地址 
    private Client client; 					// 客户端实例 
    private boolean isClosed = false;  		// 客户端是否关闭 

    private Invoker(Class<? extends VersionedProtocol> protocol, InetSocketAddress address, UserGroupInformation ticket,
        Configuration conf, SocketFactory factory, int rpcTimeout, RetryPolicy connectionRetryPolicy) throws IOException {
      this.remoteId = Client.ConnectionId.getConnectionId(address, protocol, ticket, rpcTimeout, connectionRetryPolicy, conf);
      this.client = CLIENTS.getClient(conf, factory);
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      // 构造一个RPC.Invocation实例作为参数传递给调用程序, 执行调用, 返回值为value
      ObjectWritable value = (ObjectWritable) client.call(new Invocation(method, args), remoteId);
      return value.get();  // 返回调用处理结果
    }
    
    /* close the IPC client that's responsible for this invoker's RPCs 关闭client */ 
    synchronized private void close() {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }
  }
```


在Java动态代理例子中客户端代码(不是指Client类, 而是DynamicProxyClient的main方法)通过调用getProxy获得代理类proxy, 当调用代理类proxy的方法时, 代理对象proxy会将方法调用转发给InvocationHandler的实现类的invoke方法. invoke()方法中里有这么一句: method.invoke(obj, args)通过反射调用obj类的method方法, 并传入参数args.  而上面(Hadoop RPC)和Java RPC的invoke()方法中却没有这一句, 取而代之的是client.call(new Invocation()).

其实使用method.invoke(obj, args)是在本地JVM中调用; 而在Java RPC和Hadoop RPC中, 因为涉及到的是远程过程调用RPC, 所以不能在本地JVM执行,  
要将数据(Invocation对象)发送给服务端, 服务端将处理的结果再返回给客户端, 所以这里的invoke()方法必然需要进行网络通信.  
要让服务端能知道客户端想要调用的是哪个接口的哪个方法: 方法和方法的参数封装在Invocation对象里, 注意这里并没有封装要调用的接口(为什么?).  
接口protocol和要调用的地址remoteAddress等封装为Client的内部类ConnectionId, 该类能唯一确定一个连接:<remoteAddress, protocol, ticket>.  

Invoker的构造函数和invoke方法最好和RPC.getProxy方法一起分析, 因为客户端代码会先调用getProxy(), 然后调用代理类的方法时会调用到invoke方法.  
我们先把Invoker.invoke方法里的client.call具体怎么进行网络通信的调用流程放在一边. 先来分析getProxy()  

###getProxy()

```
  /** Construct a client-side proxy object that implements the named protocol,talking to a server at the named address. */
  public static VersionedProtocol getProxy(Class<? extends VersionedProtocol> protocol, long clientVersion, InetSocketAddress addr, 
 	  UserGroupInformation ticket, Configuration conf, SocketFactory factory, int rpcTimeout, RetryPolicy connectionRetryPolicy) throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      SaslRpcServer.init(conf);
    }
    final Invoker invoker = new Invoker(protocol, addr, ticket, conf, factory, rpcTimeout, connectionRetryPolicy);
    VersionedProtocol proxy = (VersionedProtocol)Proxy.newProxyInstance(protocol.getClassLoader(), new Class[]{protocol}, invoker);
    long serverVersion = proxy.getProtocolVersion(protocol.getName(), clientVersion);
    if (serverVersion == clientVersion) {
      return proxy;
    } else {
      throw new VersionMismatch(protocol.getName(), clientVersion, serverVersion);
    }
  }
```


**Class参数与返回值**  
Java动态代理在客户端代码中获得的代理类转型为Subject:  

```
    Class<?>[] interfaces = realSubject.getClass().getInterfaces(); // --> Subject.class
    Subject proxy = (Subject) Proxy.newProxyInstance(loader, interfaces, handler);
```


Java RPC的getProxy方法接收Class<T> clazz, 获得的代理类转型为T  

```
    public static <T> T getProxy(final Class<T> clazz,String host,int port) {
 		  return (T) Proxy.newProxyInstance(RPC.class.getClassLoader(), new Class[] {clazz}, handler);
    }
```


客户端代码调用:   `Echo echo = RPC.getProxy(Echo.class, "127.0.0.1", 20382);`  
所以实际上等价于: 	`Echo echo = (Echo)Proxy.newProxyInstance(loader, new Class[] {Echo.class}, handler);`  

因为Proxy.newProxyInstance的第二个参数需要接口类型, 所以getProxy方法(仅仅是一个帮助方法, 可以不用getProxy, 直接使用Proxy.newProxyInstance  
比如Java动态代理)需要传递一个接口类型. 传递的接口类型(.class)就是Proxy.newProxyInstance的返回值, 表示代理类是对该接口的一个代理.  

Hadoop的getProxy的参数类型是Class<? extends VersionedProtocol> protocol, 可以传递任何继承于VersionedProtocol的接口类型,返回值为VersionedProtocol.  
因为Hadoop中通信的协议接口都继承于VersionedProtocol接口. 所以可以返回所有协议接口都能够认识的VersionedProtocol.  

**参数:版本**  
当然也可以用Java RPC的泛型方式. 不过使用VersionedProtocol的目的是在返回代理类的对象之前(即建立IPC连接之前, 没有代理类, 就不能调用代理类的方法,  
就不能进行IPC通信), 首先检查通信的双方是否使用了相同的版本号, 在获得代理类之后, 还没返回之前, 有代理类发起到客户端的getProtocolVersion方法调用,  
传递接口名称和客户端的版本号, 获取服务器上的版本号, 如果客户端版本和服务器版本相同, 则可以返回这个代理类,  
下面就可以进一步开始IPC通信了.如果版本不符则报错VersionMisMatch.  


**调用者**  
下面看看getProxy的调用者, 注意观察传递的参数类型:  
![getProxy](https://n4tfqg.blu.livefilestore.com/y2pL_6fgoHWTDeQuaVie8BSbQjEGl48ijIjpvIgn9OCXLhOptkTcIj1u5Z69rvvU-67yFuEsWltQqskN1Z_jY2-11H6nl9xyzJBnGEO-mbhPcnm3JsdCq9H7UnAg5NhVbB_/4-4%20getProxy.png?psid=1)  

传递ClientProtocol接口类型, 因为getProxy的返回类型是VersionedProtocol, 所以可以转型为ClientProtocol. 实际上该调用等价于:  

```
    public static ClientProtocol getProxy(final Class ClientProtocol.class,String host,int port) {
      return (ClientProtocol) Proxy.newProxyInstance(RPC.class.getClassLoader(), new Class[] {ClientProtocol.class}, handler);
    }
```

因为Hadoop的通信有很多接口, 比如DatanodeProtocol, ClientDatanodeProtocol等, 如果针对每个接口类型都写一个这样的方法也是可以的,  
为了统一Hadoop就设计了VersionedProtocol协议接口, 所有的协议都继承该接口, 同时RPC.getProxy也可以设计成统一的Class<? extends VersionedProtocol>  
对于具体到getProxy(Class protocolClass)的调用, 就要传递具体的继承于VersionedProtocol的接口(如果就是传递VersionedProtocol显然没有意义)即具体的协议接口,  
并且最终返回类型的可以转型为具体的协议接口, 而不需要是VersionedProtocol.  

上面getProxy的调用者分别是DFSClient和JobClient, 即HDFS文件系统操作的客户端, 或者MapReduce任务的客户端. 前面在Java动态代理/Java RPC的客户端代码DynamicProxyClient/启动客户端测试中都是在客户端代码中获得接口的代理类, 说明代理类是在客户端生成的.而Java RPC创建RPCServer的代码new RPC.Server()是在启动服务器中完成的. 所以Hadoop中RPC.getProxy()方法的调用者是客户端代码, RPC.getServer()方法的调用者在服务器端完成.

getProxy返回的是一个代理类, 代理类是对传入的接口类型的一个代理. 当调用代理类的方法时, 最终调用的是接口的实现类的方法.  
也就是说对传入getProxy的接口类型, 最后还是要找到该接口的实现类的.  
**DFSClient要和NameNode进行通讯, 也就是要能够调用到NameNode的方法, 因此DFSClient类需要持有NameNode的一个引用. 但是因为是在分布式环境下,  
NameNode类并不存在在DFSClient相同的本地JVM上. 所以只能通过NameNode的接口(ClientPrototol), 接口是客户端和服务器能够共同访问的.  
怎么获取到协议接口, 就是通过这里介绍的getProxy()方法从而获得接口的一个代理类. 这个代理类负责调用到NameNode上的方法.**  


###waitForProxy()

```
  /** Get a proxy connection to a remote server 获取到一个到远程服务器的代理连接, 
   * 客户端调用该方法得到接口(协议接口)的代理对象(返回的是协议接口,实际得到的是协议接口的实现的一个代理, 面向接口编程), 
   * 然后利用该代理对象调用接口的方法(暴露给客户端的只是接口). 
   * 之后的操作交给了RPC处理, RPC的客户端会发送调用到远程服务器进行处理并返回结果给客户端. 
   * 在客户端看来好像是在本地调用的, RPC隐藏了客户端和服务器之间的交互.使客户端调用代码变得简单.
   * @param protocol protocol class 协议类 
   * @param clientVersion client version 客户端版本 
   * @param addr remote address 远程地址 
   * @param conf configuration to use 使用配置类实例 
   * @param connTimeout time in milliseconds before giving up 超时时间 
   * @return the proxy 返回代理  */
  static VersionedProtocol waitForProxy(Class<? extends VersionedProtocol> protocol, long clientVersion, InetSocketAddress addr,
	    Configuration conf, int rpcTimeout, long connTimeout) throws IOException { 
    while (true) {
      try {
        return getProxy(protocol, clientVersion, addr, conf, rpcTimeout);
      } catch(ConnectException se) {  // namenode has not been started
      } catch(SocketTimeoutException te) {}  // namenode is busy
      if (System.currentTimeMillis()-connTimeout >= startTime) { // check if timed out
        throw ioe;
      }
      try {
        Thread.sleep(1000); // wait for retry
      } catch (InterruptedException ie) {} // IGNORE
    }
  }
```


###getServer() & RPC.Server
在getProxy中我们说: Hadoop中RPC.getProxy()方法的调用者是客户端代码, RPC.getServer()方法的调用者在服务器端完成. 在比较Hadoop RPC和Java RPC中我们也对比了创建RPC Server的不同方式. 现在具体来看getServer是如何完成创建RPC服务器的操作的.

```
  public static Server getServer(final Object instance, final String bindAddress, final int port, Configuration conf)throws IOException {
    return getServer(instance, bindAddress, port, 1, false, conf);
  }
  public static Server getServer(final Object instance, final String bindAddress, final int port, final int numHandlers, final boolean verbose, Configuration conf){
    return getServer(instance, bindAddress, port, numHandlers, verbose, conf, null);
  }
  /** Construct a server for a protocol implementation instance listening on a port and address, with a secret manager.
   * @param instance 协议实现类
   * @param bindAddress RPC要绑定的地址, 服务器端的地址, 客户端以该地址链接服务器
   * @param port 绑定地址对应的端口
   * @param numHandlers 处理器的数量
   * @param verbose 是否打印详细信息
   * @param conf 配置
   * @param secretManager
   * @return RPC服务器 */
  public static Server getServer(final Object instance, final String bindAddress, final int port,
      final int numHandlers, final boolean verbose, Configuration conf, SecretManager<? extends TokenIdentifier> secretManager) {
    return new Server(instance, conf, bindAddress, port, numHandlers, verbose, secretManager);
  }
  
  /** An RPC Server. */
  public static class Server extends org.apache.hadoop.ipc.Server {
    private Object instance;
    private boolean verbose;

    /** Construct an RPC server.
     * @param instance the instance whose methods will be called 被调用的方法的实例(实现类,不是接口)
     * @param conf the configuration to use 配置文件
     * @param bindAddress the address to bind on to listen for connection 为了监听连接,绑定的服务器地址
     * @param port the port to listen for connections on 服务器端口
     * @param numHandlers the number of method handler threads to run 方法调用线程
     * @param verbose whether each call should be logged 是否打印详细信息*/
    public Server(Object instance, Configuration conf, String bindAddress, int port, int numHandlers, boolean verbose,SecretManager secretManager){
      super(bindAddress, port, Invocation.class, numHandlers, conf, classNameBase(instance.getClass().getName()), secretManager);
      this.instance = instance;
      this.verbose = verbose;
    }
    public Server(Object instance, Configuration conf, String bindAddress, int port) throws IOException {
      this(instance, conf,  bindAddress, port, 1, false, null);
    }
  }
```


仅仅通过上面的方法我们还不能确定Object instance对象到底是什么对象,实现类还是接口? 查看第三个getServer的调用树(前两个重载方法用于测试):
![getServer](https://n4tfqg.blu.livefilestore.com/y2puOQa85yN-APRhVn1TYxWf8YHS52A-6sDYUoycWrzOLxb2xed7XIXRZKoJegyICBw4-aonvWZ-JZJTSvJMZeHfrSjuq0_Gb8pi9f0N45u_dsod0ekSnm6jkle1pNxSFZL/4-5%20getServer.png?psid=1)  


**NameNode.getServer()**
NameNode中有两个Server实例, 对于客户端的连接使用Server server, 对于DataNode的连接使用配置的serviceRpcServer. 由此可以看出NameNode的角色主要是作为一个RPC服务器, 用来接收客户端或者DataNode的连接请求. getServer的this参数指的是当前类的实例对象的引用.  

```
  private Server server; // RPC server:NameNode的RPC服务器实例
  private Server serviceRpcServer;
  private InetSocketAddress serverAddress = null; /** RPC server address */
  protected InetSocketAddress serviceRPCAddress = null; /** RPC server for DN address */
 
  /** Initialize name-node. */
  private void initialize(Configuration conf) throws IOException {
    // create rpc server
    InetSocketAddress dnSocketAddr = getServiceRpcServerAddress(conf);
    if (dnSocketAddr != null) {
      int serviceHandlerCount = conf.getInt(DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY, 
    		  DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_DEFAULT);
      this.serviceRpcServer = RPC.getServer(this, dnSocketAddr.getHostName(), dnSocketAddr.getPort(), 
    		  serviceHandlerCount, false, conf, namesystem.getDelegationTokenSecretManager());
      this.serviceRPCAddress = this.serviceRpcServer.getListenerAddress();
      setRpcServiceServerAddress(conf);
    }

    InetSocketAddress socAddr = NameNode.getAddress(conf);
    this.server = RPC.getServer(this, socAddr.getHostName(), socAddr.getPort(), 
    		handlerCount, false, conf, namesystem.getDelegationTokenSecretManager());

    this.server.start();  //start RPC server 启动RPC服务器, 调用的是Server抽象类的start方法   
    if (serviceRpcServer != null) {
      serviceRpcServer.start();      
    }
  }
```


NameNode作为IPC服务器接收客户端(server变量)/DataNode(serviceRpcServer)的连接请求


**DataNode.getServer()**

```
  public Server ipcServer; // For InterDataNodeProtocol 内部datanode调用的ipc服务器

  void startDataNode(Configuration conf, AbstractList<File> dataDirs, SecureResources resources) throws IOException {
    // init ipc server 初始化内部hadoop ipc服务器
    InetSocketAddress ipcAddr = NetUtils.createSocketAddr(conf.get("dfs.datanode.ipc.address"));
    ipcServer = RPC.getServer(this, ipcAddr.getHostName(), ipcAddr.getPort(), 3, false, conf, blockTokenSecretManager);
    dnRegistration.setIpcPort(ipcServer.getListenerAddress().getPort());
  }
```


DataNode作为IPC服务器处理内部DataNode的请求.


**JobTracker.getServer()**

```
  Server interTrackerServer;

  JobTracker(final JobConf conf, String identifier, Clock clock, QueueManager qm) {
    InetSocketAddress addr = getAddress(conf); // Set ports, start RPC servers, setup security policy etc.
    this.interTrackerServer = RPC.getServer(this, addr.getHostName(), addr.getPort(), 10, false, conf, secretManager);
  }
  public void offerService() throws InterruptedException, IOException { /** Run forever */
    this.interTrackerServer.start(); // start the inter-tracker server
  }
```


**TaskTracker.getServer()**

```
  Server taskReportServer = null;

  synchronized void initialize() throws IOException, InterruptedException {
    // bind address
    String address = NetUtils.getServerAddress(fConf,
		"mapred.task.tracker.report.bindAddress", "mapred.task.tracker.report.port", "mapred.task.tracker.report.address");
    InetSocketAddress socAddr = NetUtils.createSocketAddr(address);

    // RPC initialization
    int max = maxMapSlots > maxReduceSlots ? maxMapSlots : maxReduceSlots;
    //set the num handlers to max*2 since canCommit may wait for the duration of a heartbeat RPC
    this.taskReportServer = RPC.getServer(this, socAddr.getHostName(), socAddr.getPort(), 2 * max, false, this.fConf, this.jobTokenSecretManager);
    this.taskReportServer.start();
  }
```



上面四个组件NN, DN, JT, TT都在初始化方法中调用getServer获取IPC Server实例, 然后调用start()启动RPC Server.  
RPC Server指的是ipc.Server抽象类, IPC Server表示ipc.RPC.Server内部类. 实际上getServer()会调用new RPC.Server(), 最终返回的是Server抽象类.  

RPC.getServer的Object instance参数都是传入当前类的对象引用this. 当前类都实现了该组件和其他组件进行通讯的协议接口.  
getServer() 调用new Server(instance,..) 将传入的对象引用this保存在RPC.Server的成员变量Object instance中. Instance变量用于RPC.Server.call()方法  


###RPC.Server.call()

```
    public Writable call(Class<?> protocol, Writable param, long receivedTime) throws IOException {
        Invocation call = (Invocation)param; //将参数转换为Invocation对象
        Method method = protocol.getMethod(call.getMethodName(), call.getParameterClasses()); //取得Invocation中的方法名和参数
        method.setAccessible(true);
        Object value = method.invoke(instance, call.getParameters()); //反射调用: instance为接口的实现类.调用instance的method,参数为call.parameters
        return new ObjectWritable(method.getReturnType(), value); //将RPC调用的返回结果封装成ObjectWritable类型
    }
```


###RPC.call()
来看RPC的最后一个没有分析到的call方法, 注意不是RPC.Server.call(). 该方法最主要的是client.call(), Invoker的invoke()里也调用了client.call()  
该方法主要用于并行调用. 从Call Hierarchy可以看出只有TestRPC用到该方法. 该方法和Invoker.invoke()一样最终返回客户端的调用结果.  

```
  /** Expert: Make multiple, parallel calls to a set of servers. */
  public static Object[] call(Method method, Object[][] params,InetSocketAddress[] addrs, UserGroupInformation ticket, Configuration conf) {
    Invocation[] invocations = new Invocation[params.length]; // 一组方法调用实例 
    for (int i = 0; i < params.length; i++)  invocations[i] = new Invocation(method, params[i]);
    Client client = CLIENTS.getClient(conf); // 创建并缓存一个org.apache.hadoop.ipc.Client实例 
    try {
      Writable[] wrappedValues = client.call(invocations, addrs, method.getDeclaringClass(), ticket, conf); // 根据参数, 客户端发送调用方法及其参数 
      if (method.getReturnType() == Void.TYPE)  return null;
      Object[] values = (Object[])Array.newInstance(method.getReturnType(), wrappedValues.length); // 客户端执行RPC调用, 获取到返回值 
      for (int i = 0; i < values.length; i++)
        if (wrappedValues[i] != null)
          values[i] = ((ObjectWritable)wrappedValues[i]).get(); // 获取返回值的实例 
      return values;
    } finally {
      CLIENTS.stopClient(client); // 如果该client实例的引用计数为0, 该client就被关闭 
    }
  }
```


###Hadoop RPC & Java RPC
![HadoopRPC-1](https://n4tfqg.blu.livefilestore.com/y2pz0RDUIPDgLKEK6FcK3LwenqJQHh2FFkHTiwGmCqiAw7o3VuqjMfEtcF-SZdkNNpUwbubrSulAIRDvHpb57ym2RO3F_iwI2nRvwDnzzpgkmjcJp7j2lbJxs6Xns0jYppw/4-6%20HadoopRPC(1).png?psid=1)  

![HadoopRPC-2](https://n4tfqg.blu.livefilestore.com/y2pTQwl9UJ49zLX3EQt35h0mVsq2WBpIEFbRkwUQP7ktdo9pwIQpcnMl_R8URR255UtYR57cCH3aNP8l29Wu_ZeCI0cuzUV0gZhnVBPamJOFagUF-nnsAds-SAoIpsO_OGy/4-6%20HadoopRPC(2).png?psid=1)  
