---
layout: post
title: Hadoop源码分析之RPC Client
category: Source
tags: BigData
keywords: 
description: 
---

前面在分析RPC时和Client相关的代码在Invoker中(Client, Client.ConnectionId). Invoker.invoke()完成的是DFSClient调用代理类时的调用转发操作.  
在invoke()里调用client.call(new Invocation(method, args), remoteId) 流程进入Client的处理. 注意: 这里的Client不是DFSClient.  
Invoker中client的获取从ClientCache中得到, 如果不在Cache中则创建Client: new Client(ObjectWritable.class, conf, factory)  

![5-1 Client method](https://n4tfqg.blu.livefilestore.com/y2pfWi8lrZhPcOghlU3xLN5HrClnzrlKjKd_DCSLkjP0L-rD_b4BiWW_eFN-n4OlJsWhU2l4hCnZvAhY7hjUUw-HuBQV4aTfdbxYFgdba4u7UCR3Uex0b4rOcRaUyqpECTF/5-1%20Client%20method.png?psid=1)  

###Client
{% highlight java %}
/** A client for an IPC service.  
 * IPC calls take a single Writable as a parameter, and return a Writable as their value. ==> (ObjectWritable)client.call() 
 * A service runs on a port and is defined by a parameter class and a value class. */
public class Client {
  private Hashtable<ConnectionId, Connection> connections = new Hashtable<ConnectionId, Connection>(); //客户端维护到服务端的一组连接 
  private AtomicBoolean running = new AtomicBoolean(true); // if client runs 客户端进程是否允许
  private Class<? extends Writable> valueClass;   	// class of call values
  private int counter;                      	// counter for call ids
  final private Configuration conf;				// 配置类实例 
  private SocketFactory socketFactory;          	// how to create sockets  Socket工厂,用来创建Socket连接 
  private int refCount = 1;

  /** Construct an IPC client whose values are of the given Writable class. */
  public Client(Class<? extends Writable> valueClass, Configuration conf, SocketFactory factory) {
    this.valueClass = valueClass;
    this.conf = conf;
    this.socketFactory = factory;
  }
  // Construct an IPC client with the default SocketFactory
  public Client(Class<? extends Writable> valueClass, Configuration conf) {
    this(valueClass, conf, NetUtils.getDefaultSocketFactory(conf));
  }
 
  /** Return the socket factory of this client */
  SocketFactory getSocketFactory() {
    return socketFactory;
  }
}
{% endhighlight %}

Client最重要的属性是connections, 一个Clinet主要处理的是与服务端进行连接的工作, 包括连接的创建、监控等.  
connections是一个Hashtable, 是线程安全的Map. Map的key是ConnectionID, 能确定唯一的连接, value是Connection, 代表此次连接的具体信息.  

####工作原理
由于Client可能和多个Server通信(一个Client会有多个Client.Connection), 典型的一次HDFS读, 需要和NameNode打交道, 也需要和某个/某些DataNode通信. 这意味着某一个Client需要维护多个连接. 同时为了减少不必要的连接, Client的做法是拿ConnectionId来做为Connection的ID. ConnectionId包括一个InetSocketAddress（IP地址/主机名+端口号）对象和一个用户信息对象. 即同一个用户到同一个InetSocketAddress的通信将共享同一个连接. 在Hadoop新的版本中加入了protocol, 即还要加上接口类型, 才能确定唯一的连接.  

连接被封装在类Client.Connection中, 所有的RPC调用, 都是通过Connection进行通信. 一个RPC调用, 有输入参数, 输出参数和可能的异常, 同时为了区分在同一个Connection上的不同调用, 每个调用都有唯一的id. 调用是否结束也需要一个标记, 所有的这些都体现在对象Client.Call中. Connection对象通过一个Hash表, 维护在这个连接上的所有Call(不是Client.connections, 而是Client.Connection.calls).  

一个RPC调用通过addCall, 把请求加到Connection里. 为了能够在这个框架上传输Java的基本类型, String和Writable接口的实现类, 以及元素为以上类型的数组, 我们一般把Call需要的参数param打包成为ObjectWritable对象, 对此次Call的返回值value也是Writable类型.  

Connection会通过socket连接服务器, 连接成功后会校验客户端/服务器的版本号Connection.writeHeader(), 校验成功后就可以通过Writable对象来进行请求的发送/应答了. 注意, 每个Client.Connection会起一个线程, 不断去读取socket, 并将收到的结果解包, 找出对应的Call, 设置Call并通知结果已经获取. Call使用Obejct的wait和notify, 把RPC上的异步消息交互转成同步调用.  

####流程入口
从RPC.Invoker.invoke: client.call(new Invocation(method, args), remoteId)为出发点, 开始分析Client的流程:  
**RPC.Invoker**  
{% highlight java %}
  private static class Invoker implements InvocationHandler {
    private Client.ConnectionId remoteId;
    private Client client;

    private Invoker(Class<? extends VersionedProtocol> protocol, InetSocketAddress address, UserGroupInformation ticket,
        Configuration conf, SocketFactory factory, int rpcTimeout, RetryPolicy connectionRetryPolicy) {
      this.remoteId = Client.ConnectionId.getConnectionId(address, protocol, ticket, rpcTimeout, connectionRetryPolicy, conf);
      this.client = CLIENTS.getClient(conf, factory);
    }
    
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      ObjectWritable value = (ObjectWritable)client.call(new Invocation(method, args), remoteId);
      return value.get();
    }
  }
{% endhighlight %}

**Client.call**  
{% highlight java %}
  public Writable call(Writable param, ConnectionId remoteId) throws InterruptedException, IOException {
    Call call = new Call(param);
    Connection connection = getConnection(remoteId, call);
    connection.sendParam(call);                 // send the parameter
  }
{% endhighlight %}

![5-2 Client Connection](https://n4tfqg.blu.livefilestore.com/y2p2zEE5dw9oLSiSbWzg52kVsTbWdMV3-X_XQ3jTUXYTn0fT0H1DDIZU7iUT44yjHseFbhdhwN2b-dZRDA4vkS2qEUC0KTNMewhj1nNIRX-W_e0RFabdMxvdov_D9RkzpWa/5-2%20Client.Connection.png?psid=1)  

###Call
{% highlight java %}
  /** A call waiting for a value. */
  private class Call {
    int id;            	// call id 调用ID
    Writable param;   	// parameter 参数(Invocation对象封装的RPC调用的方法名, 参数值, 参数类型)
    Writable value;    	// value, null if error  RPC调用返回的值
    IOException error;  	// exception, null if value 调用的异常信息
    boolean done;     	// true when call is done 调用是否完成?

    protected Call(Writable param) {
      this.param = param;
      synchronized (Client.this) {
        this.id = counter++;	// 互斥修改法：对多个连接的调用线程进行统计 
      }
    }

    /** Indicate when the call is complete and the value or error are available.  Notifies by default. 调用完成, 设置标志, 唤醒其它线程  */
    protected synchronized void callComplete() {
      this.done = true;
      notify();      		// notify caller 当调用方法完成,通知调用者, 即Invoker-->InvocationHandler--->接口代理-->客户端DFSClient
    }
    /** Set the exception when there is an error. Notify the caller the call is done. 调用出错, 同样置调用完成标志, 并设置出错信息 */
    public synchronized void setException(IOException error) {
      this.error = error;
      callComplete();
    }
    /** Set the return value when there is no error.  Notify the caller the call is done. 调用完成, 设置调用返回的值  */
    public synchronized void setValue(Writable value) {
      this.value = value;
      callComplete();
    }
  }
{% endhighlight %}
  
Call内部类主要是对一次调用的实例进行监视与管理, 获取调用返回值value, 如果出错则获取出错信息error, 同时修改Client全局统计变量.  
Client.Call类比RPC.Invocation对象多了几个属性(id, value), 同时也多了动作的处理(setValue时线程的通知).  


####线程等待wait和通知notify  
![5-3 java thread](https://n4tfqg.blu.livefilestore.com/y2prhQkrTm9duNZGx1pnF5a0TQZu0NJIAHrIYt750b2H_ebaTz9hclDGQftx8yuIX27B5v7aV_NxyVMbnMD5Z6VD66q8EkP3XcR2vfmQif3XE8W9Xn-Lb03WJBm3Klw4a26/5-3%20java%20thread.png?psid=1)  

[Java多线程: 线程状态图](http://www.cnblogs.com/mengdd/archive/2013/02/20/2917966.html)  
1. 线程调用start()方法开始后, 就进入到可运行状态. 随着CPU的资源调度在运行和可运行之间切换; 遇到阻塞则进入阻塞状态.  
2. 当资源被一个线程访问时, 上锁(synchronized), 其他线程就进入了一个`锁池(Lock pool)`; 当锁被释放, 其他线程获得了锁, 就变为可运行状态.  
3. 线程调用了wait()方法之后, 释放掉锁, 进入`等待池(Wait pool)`; 收到通知之后等待获取锁(2), 获取锁之后才可以运行.  

####RPC调用(Call)的线程机制  
调用client.call()时, 客户端将此次RPC调用的参数发送给IPC服务器, 由服务器执行此次RPC调用. 客户端调用call()的目的是为了获得调用方法的结果. 由于一次RPC调用并不会立即返回此次调用的返回结果. 客户端在将参数发送给服务器之后, 根据Call的done布尔属性判断此次RPC调用是否已经完成了, 如果没有完成, 则继续等待. 在多线程环境下, client.call线程判断done的状态值, 而其他线程在服务器完成此次RPC的调用之后, 会将调用结果返回给客户端, 修改done=true表示此次RPC调用完成了, 所以获取每一次RPC调用的返回结果需要进行同步: 首先获得call锁. 当发现此次RPC调用还没完成, 就需要释放该RPC调用的锁. 只有释放了该Call的锁, 其他线程才有机会将call.done修改成true. 客户端代码在wait状态后, 当再次获取到该call锁后, 判断到call.done=true了, 就不会再wait了, 而是继续持有该锁, 如果接收到的调用结果没有错误就返回此次RPC调用的返回值return call.value并释放掉该call锁.

![5-4 rpc thread](https://n4tfqg.blu.livefilestore.com/y2p1vr_67W6_wr1O6kUC-JUcM9biXmOqGcL85ZO5rlDxvDMtmRAzyuKY0ERDjnZ1i41vjdjSvxgWxyBtk5sxBzxiTdWaDwVM03OueCkm8GjfV_X8wpL-JRlkAdxKMS2erYM/5-4%20RPC%20thread.png?psid=1)    
在分析getConnection(ConnectionId, Call)之前先来分析ConnectionId和Connection两个内部类. Connection尤其重要.

###ConnectionId
{% highlight java %}
   /**This class holds the address and the user ticket.The client connections to servers are uniquely identified by<remoteAddress,protocol,ticket>*/
   static class ConnectionId {
     InetSocketAddress address;	// 远程服务器地址
     UserGroupInformation ticket;	// 用户
     Class<?> protocol;			// 协议接口类

     private static final int PRIME = 16777619;
     private int rpcTimeout;
     private String serverPrincipal;
     private int maxIdleTime; //connections will be culled if it was idle for maxIdleTime msecs 连接的最大空闲时间 
     private final RetryPolicy connectionRetryPolicy;
     private boolean tcpNoDelay; // if T then disable Nagle's Algorithm 设置TCP连接是否延迟 
     private int pingInterval; // how often sends ping to the server in msecs  ping服务端的间隔  

     public boolean equals(Object obj) {
       if (obj == this)  return true;
       if (obj instanceof ConnectionId) {
         ConnectionId that = (ConnectionId) obj;
         return isEqual(this.address, that.address) && this.maxIdleTime == that.maxIdleTime
             && isEqual(this.connectionRetryPolicy, that.connectionRetryPolicy) && this.pingInterval == that.pingInterval
             && isEqual(this.protocol, that.protocol) && this.rpcTimeout == that.rpcTimeout
             && isEqual(this.serverPrincipal, that.serverPrincipal) && this.tcpNoDelay == that.tcpNoDelay
             && isEqual(this.ticket, that.ticket);
       }
       return false;
     }
  }
{% endhighlight %}

一个连接的实体类, 标识了一个连接实例的Socket地址、用户信息UserGroupInformation、连接的协议类. 每个连接都是通过一个该类的实例唯一标识  
只有当Socket地址:remoteAddress、用户信息ticket、连接的协议类protocol这三个属性的值相等时, 才被认为是同一个ConnectionId实例.  
构造一个ConnectionId通过getConnectionId(在RPC.Invoker的构造函数中调动了该方法)  
{% highlight java %}
     static ConnectionId getConnectionId(InetSocketAddress addr,
         Class<?> protocol, UserGroupInformation ticket, int rpcTimeout,
         RetryPolicy connectionRetryPolicy, Configuration conf) throws IOException {
       if (connectionRetryPolicy == null) {
         final int max = conf.getInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT);
         connectionRetryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(max, 1, TimeUnit.SECONDS);
       }
       String remotePrincipal = getRemotePrincipal(conf, addr, protocol);
       return new ConnectionId(addr, protocol, ticket,  rpcTimeout, remotePrincipal,
           conf.getInt("ipc.client.connection.maxidletime", 10000), connectionRetryPolicy,
           conf.getBoolean("ipc.client.tcpnodelay", false), Client.getPingInterval(conf));
     }
{% endhighlight %}

###ConnectionHeader
{% highlight java %}
/** The IPC connection header sent by the client to the server on connection establishment. */
class ConnectionHeader implements Writable {
  private String protocol;
  private UserGroupInformation ugi = null;
  private AuthMethod authMethod;
}
{% endhighlight %}

ConnectionId标识一个唯一的连接<address, protocol, ticket>,  ConnectionHeader为连接头信息, 在Client和Server建立连接的时候会发送连接头信息给Server:<protocol, ticket, authMethod>.  注意这里发送了protocol协议接口类型, 因为针对同一个Connection, 通过<addr, protocol, ticket>来唯一确定一个连接, 当Client对Server的某个protocol(Server可能实现了多个protocol)发起多次RPC调用, 只要针对的是同一个服务器地址, 同一个协议接口类型, 同一个用户, 那么使用的都是同一个Connection, 在发送RPC调用参数之前, 首先发送ConnectionHeader, 其中包括了此次Connection的协议接口类型, 这样每次的RPC调用就不需要再发送协议接口类型这个参数了, 这也正是Invocation对象并没有封装protocol的原因.  假设ConnectionHeader没有发送协议接口类型, 那么每一次RPC调用都要发送<protocol, method, args>这三个参数, 而针对同一个Connection的多次RPC调用, protocol值是相同的. 为了节省带宽在连接建立后connect(socket,server)就首先发送ConnectionHeader, 起到兵马未动, 粮草先行的作用. 此处的兵马=Call, 粮草=ConnectionHeader. 


###Connection
####construction
Connection: 连接管理内部线程类, 它读取(每一个Call调用实例执行后从服务端返回的)响应信息, 并通知调用者的线程(其他调用实例).  
每一个连接拥有一个连接到远程服务端的Socket连接,该Socket能够实现多路复用,使得多个调用复用该Socket,客户端收到的调用得到的响应可能是无序的.  
{% highlight java %}
  /** Thread that reads responses and notifies callers. 
   * Each connection owns a socket connected to a remote address. 
   * Calls are multiplexed through this socket: responses may be delivered out of order. */
  private class Connection extends Thread {
    private InetSocketAddress server;  	// server ip:port  (客户端要连接到的)服务端的IP地址:端口号
    private String serverPrincipal;  		// server's krb5 principal name
    private ConnectionHeader header; 	// connection header 连接头信息, 该实体类封装了连接协议,用户信息,鉴权方法 
    private final ConnectionId remoteId; 	// connection id 每一次RPC调用的唯一的ConnectionId
    private AuthMethod authMethod; 	// authentication method 授权方法
    private boolean useSasl;
    private Token<? extends TokenIdentifier> token;
    private SaslRpcClient saslRpcClient;
    
    private Socket socket = null;        	// connected socket 客户端(已经)连接的Socket
    private DataInputStream in;			// 输入流	
    private DataOutputStream out;		// 输出流
    private int rpcTimeout;
    private int maxIdleTime; 			//connections will be culled if it was idle for maxIdleTime msecs
    private final RetryPolicy connectionRetryPolicy;
    private boolean tcpNoDelay; 		// if T then disable Nagle's Algorithm
    private int pingInterval; 			// how often sends ping to the server in msecs
    
    private Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>(); // currently active calls 当前活跃的调用列表 
    private AtomicLong lastActivity = new AtomicLong();					// last I/O activity time 最后I/O活跃的时间  
    private AtomicBoolean shouldCloseConnection = new AtomicBoolean();  // indicate if the connection is closed 连接是否关闭 
    private IOException closeException; 								// close reason 连接关闭原因
     
    public Connection(ConnectionId remoteId) throws IOException {
      this.remoteId = remoteId; 			// 远程服务端连接 
      this.server = remoteId.getAddress();	// 远程服务器地址, 客户端和server建立连接采用这个地址 
      if (server.isUnresolved()) throw new UnknownHostException("unknown host: " +  remoteId.getAddress().getHostName());
      this.maxIdleTime = remoteId.getMaxIdleTime();
      this.connectionRetryPolicy = remoteId.connectionRetryPolicy;
      this.tcpNoDelay = remoteId.getTcpNoDelay();
      this.pingInterval = remoteId.getPingInterval();
      this.rpcTimeout = remoteId.getRpcTimeout();
      UserGroupInformation ticket = remoteId.getTicket();	// 用户信息 
      Class<?> protocol = remoteId.getProtocol();			// 协议 

      this.useSasl = UserGroupInformation.isSecurityEnabled();
      if (useSasl && protocol != null) {
        TokenInfo tokenInfo = protocol.getAnnotation(TokenInfo.class);
        if (tokenInfo != null) {
          TokenSelector<? extends TokenIdentifier> tokenSelector = null;
          tokenSelector = tokenInfo.value().newInstance();
          InetSocketAddress addr = remoteId.getAddress();
          token = tokenSelector.selectToken(SecurityUtil.buildTokenService(addr), ticket.getTokens());
        }
      }
      if (!useSasl) authMethod = AuthMethod.SIMPLE;
      else if (token != null) authMethod = AuthMethod.DIGEST;
      else authMethod = AuthMethod.KERBEROS;
      
      header = new ConnectionHeader(protocol == null ? null : protocol.getName(), ticket, authMethod); 	// 连接头信息 
      this.setName("IPC Client (" + socketFactory.hashCode() +") connection to " + remoteId.getAddress().toString() +
          " from " + ((ticket==null)?"an unknown user":ticket.getUserName()));
      this.setDaemon(true);	// 并设置一个连接为后台线程  
    }
  }
{% endhighlight %}

客户端所拥有的Connection实例, 通过一个远程ConnectionId实例(remoteId, 唯一标识,上面的构造函数)来建立到客户端到服务端的连接  
Client.call() 获取getConnection() 如果连接池中没有Connection, 则新建一个并加入连接池connections中.  
 
####calls operation
定义的calls集合, 是用来保存当前活跃的调用实例, 以键值对的形式保存. 因此该类Connection提供了向该集合中添加新的调用实例、移除调用实例等操作  
{% highlight java %}
 	/** 向calls集合中添加一个<Call.id, Call> 
     * Add a call to this connection's call queue and notify a listener; synchronized. Returns false if called during shutdown.
     * @param call to add 此次要被添加的RPC调用
     * @return true if the call was added. 此RPC调用被成功加入到calls中返回true */
    private synchronized boolean addCall(Call call) {
      if (shouldCloseConnection.get()) return false; 	// 在Connection关闭期间添加Call返回false, 其他情况返回true表示成功添加Call
      calls.put(call.id, call);							// 添加到Hashtable calls中. key是一个Call的id, value是Call的实例
      notify(); 									// 通知监听器(Server.Listener)线程有客户端的RPC调用来了
      return true;
    }
    
    /** wait till someone signals us to start reading RPC response or it is idle too long,
    * it is marked as to be closed,or the client is marked as not running.
　　* Return true if it is time to read a response; false otherwise. 当准备读取数据时(Server将RPC的调用结果返回给Client)返回true
    * 等待某个调用线程唤醒自己:   1. 	开始读取RPC的响应数据  2. idle空闲时间过长  3. 被标记为连接关闭  4. 客户端已经关闭. */
    private synchronized boolean waitForWork() {
      if (calls.isEmpty() && !shouldCloseConnection.get()  && running.get())  { // 2. Client运行, Connection运行, calls为空
        long timeout = maxIdleTime-(System.currentTimeMillis()-lastActivity.get());
        if (timeout>0) {
          try {
            wait(timeout);
          } catch (InterruptedException e) {}
        }
      }
      if (!calls.isEmpty() && !shouldCloseConnection.get() && running.get()) { // 1. Client运行, Connection运行, calls不为空
        return true;
      } else if (shouldCloseConnection.get()) {
        return false;
      } else if (calls.isEmpty()) { // idle connection closed or stopped
        markClosed(null);
        return false;
      } else { // get stopped but there are still pending requests 
        markClosed((IOException)new IOException().initCause(new InterruptedException()));
        return false;
      }
    }
    
    /* Receive a response. Because only one receiver, so no synchronization on in.
    * 因为每次从DataInputStream in中读取响应信息只有一个, 无需同步 */
    private void receiveResponse() {
      Call call = calls.get(id);
 	    if (state == Status.SUCCESS.state) {
          Writable value = ReflectionUtils.newInstance(valueClass, conf);
          value.readFields(in);                 // read value
          call.setValue(value);
          calls.remove(id);
      } else if (state == Status.ERROR.state) {
          calls.remove(id);
      }
    }
    
    /** Close the connection. 关闭连接, 需要迭代calls集合, 清除连接  */
    private synchronized void close() {
      if (!shouldCloseConnection.get()) { // 不应该关闭直接返回
        return;
      } 
  	  // 应该关闭则从connection连接池中移除remoteId对应的Connection, 关闭该Connection的输出流和输入流
      // release the resources  first thing to do;take the connection out of the connection list
      synchronized (connections) {
        if (connections.get(remoteId) == this) {
          connections.remove(remoteId);
        }
      }
      // close the streams and therefore the socket
      IOUtils.closeStream(out);
      IOUtils.closeStream(in);
      disposeSasl();
      cleanupCalls(); // clean up all calls
    }
    
    /* Cleanup all calls and mark them as done */
    private void cleanupCalls() {
      Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator() ;
      while (itor.hasNext()) {
        Call c = itor.next().getValue(); 
        c.setException(closeException); // local exception
        itor.remove();         
      }
    }
{% endhighlight %}

##RPC-->Client
RPC中使用动态代理, 客户端调用接口的方法会调用Invoker.invoke():

    ObjectWritable value = (ObjectWritable) client.call(new Invocation(method, args), remoteId);

客户端Client类提供的最基本的功能就是执行RPC调用, 其中, 提供了两种调用方式, 一种就是串行单个调用, 另一种就是并行调用  
client.call()将数据从客户端向服务端发送要解决的问题:  主要过程是建立连接和进行RPC调用  
① 客户端和服务端的连接是怎样建立的?  		-->2.3.4  
② 客户端是怎样给服务端发送数据的?			    -->5.  
③ 客户端是怎样获取服务端的返回数据的?		-->6.  

###1. Client.call(param, remoteId)
**RPC方法调用**  
{% highlight java %}
  /** Make a call, passing param, to the IPC server defined by remoteId, returning the value. 
  * 执行一个调用, 通过传递参数值param到运行在addr上的IPC服务器, IPC服务器基于protocol与用户的ticket来认证, 并响应客户端
  * @param param 第一个参数Invocation实现了Writable, 作为Call的一部分 */
  public Writable call(Writable param, ConnectionId remoteId) throws InterruptedException, IOException {
    Call call = new Call(param);  							// 使用请求参数值构造一个Call实例, 将传入的数据封装成Call对象
    Connection connection = getConnection(remoteId, call); 	// ① 从连接池connections中获取到一个连接（或可能创建一个新的连接）  
    connection.sendParam(call);  							// ② send the parameter 向IPC服务端发送call对象 
    boolean interrupted = false;
    synchronized (call) {
      while (!call.done) {
        try {
          call.wait();    // wait for the result 等待服务器响应,返回结果. 在Call类的callComplete()方法里有notify()方法用于唤醒线程 
        } catch (InterruptedException ie) {  // save the fact that we were interrupted
          interrupted = true;
        }
      }
      if (interrupted) { // set the interrupt flag now that we are done waiting 因中断异常而终止, 设置标志interrupted为true 
        Thread.currentThread().interrupt();
      }
      if (call.error != null) {
        if (call.error instanceof RemoteException) {
          call.error.fillInStackTrace();
          throw call.error;
        } else { // local exception: use the connection because it will reflect an ip change, unlike the remoteId 本地异常
          throw wrapException(connection.getRemoteAddress(), call.error);
        }
      } else {
        return call.value; // 调用返回的响应值  
      }
    }
  }
{% endhighlight %}

还有一个并行RPC调用的重载方法  
{% highlight java %}
  /** Makes a set of calls in parallel. 执行并行调用  Each parameter is sent to the corresponding address. 每个参数都被发送到相关的IPC服务器 
   * When all values are available, or have timed out or errored, the collected results are returned in an array. 然后等待服务器响应信息
   * The array contains nulls for calls that timed out or errored.  */
  public Writable[] call(Writable[] params, InetSocketAddress[] addresses, Class<?> protocol, UserGroupInformation ticket, Configuration conf){
    if (addresses.length == 0) return new Writable[0];
    ParallelResults results = new ParallelResults(params.length); // 根据待调用的参数个数来构造一个用来封装并行调用返回值的ParallelResults对象 
    synchronized (results) {
      for (int i = 0; i < params.length; i++) {
        ParallelCall call = new ParallelCall(params[i], results, i);	// 构造并行调用实例 
        try {
          ConnectionId remoteId = ConnectionId.getConnectionId(addresses[i], protocol, ticket, 0, conf);
          Connection connection = getConnection(remoteId, call); // 从连接池中获取到一个连接 
          connection.sendParam(call);  	// send each parameter 发送调用参数 
        } catch (IOException e) {
          results.size--;             		//  wait for one fewer result 更新统计数据 
        }
      }
      while (results.count != results.size) {
        try {
          results.wait();               	// wait for all results 等待一组调用的返回值（如果调用失败会返回错误信息或null值, 但与计数相关） 
        } catch (InterruptedException e) {}
      }
      return results.values; // 调用返回一组响应值 
    }
  }
{% endhighlight %}

###2. Client.getConnection(remoteId, call) 
**得到连接**  
{% highlight java %}
  /** Get a connection from the pool, or create a new one and add it to the pool. Connections to a given ConnectionId are reused. */
  private Connection getConnection(ConnectionId remoteId, Call call) throws IOException, InterruptedException {
    if (!running.get()) {throw new IOException("The client is stopped");} // the client is stopped 连接关闭
    Connection connection;
    //we could avoid this allocation for each RPC by having a connectionsId object and with set() method.通过ConnectionId,不用每次RPC调用都new一个连接  
    //We need to manage the refs for keys in HashMap properly. For now its ok. 将ConnectionId作为连接池Map的key来管理连接 
    //如果connections连接池中有对应的连接对象, 就不需重新创建了; 如果没有就需重新创建一个连接对象.   
    //但请注意, 该连接对象只是存储了remoteId的信息, 其实还并没有和服务端建立连接
    do {
      synchronized (connections) {
        connection = connections.get(remoteId);
        if (connection == null) {
          connection = new Connection(remoteId);
          connections.put(remoteId, connection);
        }
      }
    } while (!connection.addCall(call)); // 将call对象放入对应连接中的calls池
    // 1). get the Connection from connections 从连接池中获得Connection对象
    //    while connection.addCall(call) return false, !connection.addCall(call)=true, then do{} again.
    //    while connection.addCall(call) return true, then exist loop, and execute below code
    // 2). connection.addCall(call)只有在关闭Connection的时候才返回false, 其他情况都返回true.
    // 3). 将此次RPC Call加入calls中, 设置该Connection的IO流
    // 同一个Connection的多次RPC调用, 会加入到calls中. 每一次RPC调用, 都会调用connection.setupIOstreams.. 
     
    // we don't invoke the method below inside "synchronized (connections)" block above. 不在上面的同步块中完成下面的方法操作
    // The reason for that is if the server happens to be slow, 因为服务器建立一个连接会耗费比较长的时间,在同步块中执行会影响系统运行
    // it will take longer to establish a connection and that will slow the entire system down.
    connection.setupIOstreams(); // 和服务端建立连接
    return connection;
  }
{% endhighlight %}

在获取到Connection对象之后, 客户端开始和服务端建立连接, 下面的方法都是Connection类中的方法. 通过connection.setupIOstreams()建立IO流, 准备数据传输.  方法中出现的比较重要的变量是针对此Connection连接用到的Socket, DataInputStream, DataOutputStream.  
  
###3. connection.setupConnection()
**建立连接**  
{% highlight java %}
    private synchronized void setupConnection() throws IOException {
      short ioFailures = 0;
      short timeoutFailures = 0;
      while (true) {
        try {
          this.socket = socketFactory.createSocket(); // 创建连接用的Socket
          this.socket.setTcpNoDelay(tcpNoDelay); 
          NetUtils.connect(this.socket, server, 20000); // connection time out is 20s 开始连接
          if (rpcTimeout > 0) {
            pingInterval = rpcTimeout;  // rpcTimeout overwrites pingInterval
          }
          this.socket.setSoTimeout(pingInterval);
          return;
        } catch (SocketTimeoutException toe) {           
          if (updateAddress()) // Check for an address change and update the local reference.
            timeoutFailures = ioFailures = 0; // Reset the failure counter if the address was changed
          handleConnectionFailure(timeoutFailures++, 45, toe); /* The max number of retries is 45, which amounts to 20s*45 = 15 minutes retries.*/
        } catch (IOException ie) {
          if (updateAddress()) 
            timeoutFailures = ioFailures = 0;
          handleConnectionFailure(ioFailures++, ie);
        }
      }
    }	// handleConnectionFailure()中会关闭Socket
    private void closeConnection() {
      try {
        socket.close();  // close the current connection 关闭当前连接
      } catch (IOException e) { LOG.warn("Not able to close a socket", e);}
      socket = null; // set socket to null so that the next call to setupIOstreams can start the process of connect all over again.
    }
{% endhighlight %}

###4. connection.setupIOstreams() 
**设置IO流, 启动连接线程**  
{% highlight java %}
    /** Connect to the server and set up the I/O streams. 
　　 * It then sends a header to the server and starts the connection thread that waits for responses. 发送header给服务端,启动Connection线程等待返回
     * 客户端和服务器建立连接, 然后客户端会一直监听服务端传回的数据. 和05例子的监听器类似 */
    private synchronized void setupIOstreams() throws InterruptedException {
      if (socket != null || shouldCloseConnection.get()) return;
      try {
        short numRetries = 0;
        final short maxRetries = 15;
        Random rand = null;
        while (true) {
          setupConnection(); 										// 建立连接, 在该方法中创建了Socket连接, 并且开始连接Server 
          InputStream inStream = NetUtils.getInputStream(socket); 		// 获得输入流(接收数据) 
          OutputStream outStream = NetUtils.getOutputStream(socket); 	// 获得输出流(发送数据) 
          writeRpcHeader(outStream);								// 
          // ... 使用Sasl安全机制的连接设置
          this.in = new DataInputStream(new BufferedInputStream(new PingInputStream(inStream))); //将输入流装饰成DataInputStream 
          this.out = new DataOutputStream(new BufferedOutputStream(outStream)); //将输出流装饰成DataOutputStream 
          writeHeader();											// 
          touch(); // update last activity time 更新最近活动的时间 
          start(); // start the receiver thread after the socket connection has been set up 连接建立启动接收线程等待服务端传回数据.Thread调用run()
          return;
        }
      } catch (Throwable t) {
        close();
      }
    }
    
    /* Write the RPC header 向输出流写入RPC调用的header*/
    private void writeRpcHeader(OutputStream outStream) throws IOException {
      DataOutputStream out = new DataOutputStream(new BufferedOutputStream(outStream)); // 手动构造RPC的Header到输出流中
      out.write(Server.HEADER.array()); 		// header: hrpc
      out.write(Server.CURRENT_VERSION); 	// version: 4
      authMethod.write(out); 				// authentication method: Connection构造函数中的AuthMethod枚举类值
      out.flush();
    }
    /* Write the protocol header for each connection.Out is not synchronized because only the first thread does this.*/
    private void writeHeader() throws IOException {
      DataOutputBuffer buf = new DataOutputBuffer(); // 在Connection中构造好的ConnectionHeader<protocol, ticket>写入到输出流中
      header.write(buf); // Write out the ConnectionHeader 将ConnectionHeader写入buf中, 再写到输出流中发送给Server
      int bufLen = buf.getLength();
      out.writeInt(bufLen); // Write out the payload length
      out.write(buf.getData(), 0, bufLen);
    }
  }
{% endhighlight %}

在setupIostreams方法中, 首先调用了setupConnection开始建立Client和Server的通讯连接, 并从建立的Socket连接中获取到输入流和输出流, 这个时候Client和Server的通讯就可以开始了. 虽然此时还没有调用sendParam进行RPC调用. 但是在建立好连接后已经可以预先从Client发送RPC的Header(writeRpcHeader)和协议的Header(writeHeader)[这两个过程和RPC的Call没有关系, 但却是必须要先完成的操作, 好比getProxy返回proxy对象之前要进行版本的比对], Server端也可以开始接收数据了.  

writeRpcHeader发送的数据Server会进行版本验证和鉴权. writeHeader发送的ConnectionHeader包括了<protocol, ticket, authMethod>比较重要的是protocol即接口类型, 此时发送了protocol, 所以在RPC.Invocation中不需要将接口类型封装到类中. 因为在sendParam之前已经通过writeHeader将接口类型发送给Server了.  

实际上在setupConnection之前(getConnection里)已经将客户端要调用的RPC Call加入到了calls集合中并通知调用者, 即Server端的Listener已经可以开始监听工作了. 具体流程等到Server再串起来分析.  

####touch()
使用到了java.util.concurrent.atomic包中的一些工具, 像AtomicLong、AtomicBoolean, 这些类能够以原子方式更新其值, 支持在单个变量上解除锁二实现线程的安全. 这些类能够使用get方法读取volatile变量的内存效果, set方法可以设置对应变量的内存值 
{% highlight java %}
    private void touch() {
      lastActivity.set(System.currentTimeMillis()); /** Update lastActivity with the current time. */
    }
{% endhighlight %}

###5. connection.sendParam(call)
**发送数据**  
{% highlight java %}
    /** Initiates a call by sending the parameter to the remote server. 发送Call对象给远程服务器,开始RPC调用
     * Note: this is not called from the Connection thread, but by other threads. 不是由当前客户端连接的线程调用 */
    public void sendParam(Call call) {
      if (shouldCloseConnection.get())  return;
      DataOutputBuffer d=null;
      try {
        synchronized (this.out) {          
          //for serializing the data to be written 序列化Call对象,因为Call由id和传入的Invocation param组成,需要对所有属性进行序列化
          d = new DataOutputBuffer();  	//构造输出流缓冲区,用于客户端向服务端输出(写)数据
          d.writeInt(call.id);				//序列化Call的id
          call.param.write(d);				//序列化Call的param即Invocation对象
          byte[] data = d.getData();			//输出流的数据
          int dataLength = d.getLength();	//输出流的长度
          out.writeInt(dataLength);      	//first put the data length 首先写出数据的长度
          out.write(data, 0, dataLength);	//write the data 输出数据,向服务端写入数据
          out.flush();
        }
      } catch(IOException e) {
        markClosed(e);
      } finally { //the buffer is just an in-memory buffer, but it is still polite to close early
        IOUtils.closeStream(d);
      }
    }  
{% endhighlight %}

在Client和Server建立IO流的时候, Client向Server发送的是RPCHeader和协议相关的ConnectionHeader. 发送RPC调用发送的是具体的RPC调用信息了.  
在setupIOstreams中启动了Connection线程, 用于接收服务器传回的返回数据.  


###6. connection.run()
{% highlight java %}
  private class Connection extends Thread {
    // 建立连接后, start()会调用run(), 相当于05中的监听器. 监听服务端返回的数据并读取数据.
    public void run() {
      while (waitForWork()) {//wait here for work - read or close connection 等待某个连接实例空闲, 如果存在则唤醒它执行一些任务 
        receiveResponse();
      }
      close();
    }

    /* Receive a response. Because only one receiver, so no synchronization on in.接收响应 */
    private void receiveResponse() {
      if (shouldCloseConnection.get()) { return; }
      touch();
      int id = in.readInt();     		// try to read an id 阻塞读取输入流的id
      Call call = calls.get(id);		//在calls池中找到发送时的那个调用对象 
      int state = in.readInt();    	// read call status 阻塞读取RPC调用结果的状态
      if (state == Status.SUCCESS.state) {
        Writable value = ReflectionUtils.newInstance(valueClass, conf);
        value.readFields(in);   	// read value 读取调用结果.由此我们知道客户端接收服务端的输入流包含了3个数据
        call.setValue(value);		//将读取到的值赋给call对象,同时唤醒Client等待线程. value为方法的执行结果,设置value时会通知调用者 
        calls.remove(id);			//删除已处理的Call: 本次调用结束,从活动的调用Map中删除该调用
      } else if (state == Status.ERROR.state) {
        call.setException(new RemoteException(WritableUtils.readString(in),WritableUtils.readString(in)));
        calls.remove(id);
      } else if (state == Status.FATAL.state) { // Close the connection
        markClosed(new RemoteException(WritableUtils.readString(in), WritableUtils.readString(in)));
      }
    }
  }
{% endhighlight %}

接收返回数据, 从setupIOstreams中构造好的输入流中读取数据, 输入流的数据来自于Server回传给Client的. 因为最好将Client和Server之间发送和接收数据的代码放在一起分析, 会更加准确地理解他们之间交互的过程. 这个过程对sendParam方法也是适用的.  

当每次调用touch方法的时候, 都会将lastActivity原子变量设置为系统的当前时间, 更新了变量的值. 该操作是对多个线程进行互斥的, 也就是每次修改lastActivity的值的时候, 都会对该变量加锁, 从内存中读取该变量的当前值, 因此可能会出现阻塞的情况  

###总结(UML)
Client端的操作比较简单主要是建立和Server的Socket连接和向Server发送RPC调用数据(writeRpcHeader, writeHeader, sendParam).  
分析Server端的代码时, 入口是Server端接受Client的连接请求, 并接收Client发送的RPC调用请求. 这两个操作的入口都在Server.Listener中.  

![5-5 UML](https://n4tfqg.blu.livefilestore.com/y2pZ5w8eoLcRp3M6tm33FXMe4TuMJNJC3adJKQX7cOmOiMw4hOhOo7GkYWSxcTFeW9LOcMCMGvn2Qp3Jbb9qtTQPuW5q-BnV5eFcOyMBNFTzkShwYVDn7C95VHpm_j1LSZ_/5-5%20UML.png?psid=1)  
