---
layout: post
title: Hadoop源码分析之RPC Server2
category: Source
tags: BigData
keywords: 
description: 
---

###Connection
{% highlight java %}
  /** Reads calls from a connection and queues them for handling. */
  public class Connection {
    // TCP相关变量
    private SocketChannel channel;	// Socket通道  
    private Socket socket;			// 与通道channel关联的Socket 
    private String hostAddress;		// Cache the remote host & port info so that even if the socket is disconnected
    private int remotePort;			// Socket端口
    private InetAddress addr;		// Socket地址 
    private ByteBuffer data;				// Client.Connection.sendParam(Call): Call.id+Call.param
    private ByteBuffer dataLengthBuffer;	// sendParam首先写入长度, 然后写入数据. Server端也分两部分读取=>定长读取
    private ByteBuffer rpcHeaderBuffer;		// writeRpcHeader写入的RPC头信息: (HEADER, CURRENT_VERSION, authMethod)			
    
    private int dataLength;
    
    // IPC连接相关变量
    private long lastContact;
    private boolean rpcHeaderRead = false; // if initial rpc header is read 是否读取了Rpc头信息 <-- Client.Connection.writeRpcHeader()
    private boolean headerRead = false;  //if the connection header that follows version is read. 是否读取了头信息  <- Client.Connection.writeHeader()     
	ConnectionHeader header = new ConnectionHeader(); 	// writeHeader写入的连接头信息: (protocol, ticket, authMethod) 
    Class<?> protocol;									// 协议类 
    UserGroupInformation user = null;					// 用户信息...
     
    // RPC调用相关变量
    private LinkedList<Call> responseQueue;
    private volatile int rpcCount = 0; 			// number of outstanding rpcs
    
    public Connection(SelectionKey key, SocketChannel channel, long lastContact) {
      this.channel = channel;
      this.lastContact = lastContact;
      this.data = null;
      this.dataLengthBuffer = ByteBuffer.allocate(4); // sendParam: writeInt(dataLength), int占用4个字节
      this.unwrappedData = null;
      this.unwrappedDataLengthBuffer = ByteBuffer.allocate(4);
      this.socket = channel.socket();
      this.addr = socket.getInetAddress();
      if (addr == null) this.hostAddress = "*Unknown*";
      else this.hostAddress = addr.getHostAddress();
      this.remotePort = socket.getPort();
      this.responseQueue = new LinkedList<Call>();
      if (socketSendBufferSize != 0) {
        socket.setSendBufferSize(socketSendBufferSize);
      }
    }
  } 
{% endhighlight %}
  
Connection中比较重要的成员变量是SocketChannel, 构造函数的参数SocketChannel是由上面分析到的Listener.doAccept中传入的.  
客户端写入的数据服务端通过通道SocketChannel先写到缓冲区中(上面三个数据分别对应三种缓冲区). 接着就可以从缓冲区取出数据.   
这个SocketChannel是客户端向服务器发送请求数据(RPC调用)的通信通道: 在连接建立后时,Server接受Client的连接请求, 创建SocketChannel.  
然后会根据该SocketChannel建立Connection对象. Connection进行真正的读取处理. 因此下面的读取处理都是基于此通道进行数据的读取.  

####数据格式
客户端向服务端写入的数据包括client的writeRpcHeader():RPC头, writeHeader():连接头, sendParam():数据长度和数据.   Server的Connection的readAndProcess()会进行读取处理. 这三种数据的处理都放在了同一个方法里完成. Connection是怎么将对应的数据放在对应的缓冲区里?  

![Connection data](https://n4tfqg.blu.livefilestore.com/y2pJazhJd8qpQZxUtMueZhcHymyC1ypvcTNtdbV6nJulOZ7-80M7tWvM3RbF_MQd-1-cO1GgtQJ9BT5Rr0sDiV5qopvSUbcGNcg5_ediQuKF360eNRkw58g5rP-Clxwu8_M/6-5%20Connection%20data.png?psid=1)  

在分析readAndProcess方法之前, 先来看下Client发送的三种数据的格式.  
writeRpcHeader用到的数据格式,注意数据的类型,后面要根据数据类型来计算缓冲区的长度. byte=1个字节, int=4个字节, ByteBuffer有多少字符就多少字节.  
{% highlight java %}
  // The first four bytes of Hadoop RPC connections
  public static final ByteBuffer HEADER = ByteBuffer.wrap("hrpc".getBytes());
  
  // 1 : Introduce ping and server does not throw away RPCs
  // 3 : Introduce the protocol into the RPC connection header
  // 4 : Introduced SASL security layer
  public static final byte CURRENT_VERSION = 4;
  
  /** Authentication method */
  public static enum AuthMethod {
    SIMPLE((byte) 80, "", AuthenticationMethod.SIMPLE),
    KERBEROS((byte) 81, "GSSAPI", AuthenticationMethod.KERBEROS),
    DIGEST((byte) 82, "DIGEST-MD5", AuthenticationMethod.TOKEN);
  }
{% endhighlight %}

1. writeHeader和sendParam方法因为发送的都是对象数据, 分别是ConnectionHeader和Call. 而对象的长度是事先预估的. 所以采用显示长度的数据帧结构.  
2. ConnectionHeader(protocol, ticket, authMethod), 与ConnectionId(address, ticket, prototol)不同, 没有address. 因为ConnectionId是用来唯一确定一个连接的, 所以是需要Server的地址来确定一个连接. 而ConnectionHeader是作为一个连接头要发送给Server, 显然不需要发送address信息, 发送了也没用处.  
3. Call(id, param,value)对象只有(id, param)会发送到Server.  

现在来看为什么消息要设置成显示长度的类型. 因为对象中的属性的长度是不固定的. 以Call为例, 客户端发起RPC调用传递的Invocation会被作为Client.Call, 而封装的Invocation包括方法名和参数, 显然是不固定的.  设置成显示长度的做法是先写入数据的长度, 再写入长度为数据的长度的内容(数据包括长度和内容). 这样将通道的数据读取到缓冲区中, 再从缓冲区读取数据只要首先读取数据的长度(假设读取到长度为20), 接着只要再从缓冲区中读取指定的长度的字节(比如20个字节)就可以了, 也就是说客户端发送的一次RPC调用的请求数据都被Server端读取完毕了. 我们说的是RPC调用, 也就是说writeHeader和sendParam都会发起一次RPC调用. 而writeRpcHeader不是.  

再来说说Server.Connection端缓冲区的设置, 如果客户端调用的三个方法在Server端都用相应的缓冲区来表示, 则会有如下形式:  
![Connection buffer](https://n4tfqg.blu.livefilestore.com/y2pj3_avVLgPtZTygw0iZigh8B-e70IabKPl6gVsVx9_IgHKOIwyEjTI9TZ6LMfJrGXKWJ5PoEjdV4EPSUsHOhDm_vT_FQvdhgJsY8sfhM7dmXK7z3AsKfPORpuqPTPJWca/6-6%20Connection%20buffer.png?psid=1)  

这样就需要6个字节的RpcHeader, 2个lengthBuffer, 2个data Buffer. 而Hadoop的做法是最大限度地减少缓冲区的个数并且进行复用.  
上图中将相同颜色的虚线连接统一为一个缓冲区, 因此现在就只需要三个缓冲区了. 复用的前提是消息的长度都是相同的. 也就是缓冲区的大小是一样的.  

1. CURRENT_VERSION=4, byte类型, 1个字节, authMethod, 枚举类型值也为byte类型, 1个字节, 将这两个字节合并成rpcHeaderBuffer.  
2. HEADER=hrpc, ByteBuffer类型, 字符串长度正好4个字节, 而缓冲区dataLengthBuffer的长度也是4个字节, 所以HEADER可以用dataLengthBuffer来填充.  
ConnectionHeader.length和Call.length都是int类型, int占用4个字节, 所以这两个属性也都可以用同一个dataLengthBuffer来填充. 不需要再新增缓冲区.  
3. ConnectionHeader.data和Call.data使用显示长度的缓冲区, 通过计算ConnectionHeader和Call的长度,填充对应长度的字节的缓冲区data[dataLength]  

从Server.Connection属性也可看出读取Client数据时只用了3个Buffer: rpcHeaderBuffer(2个字节), dataLengthBuffer(4个字节), data(显示长度dataLenth).  

####流程分析 
{% highlight java %}
    /* Write out the RPC header: header, version and authentication method */
    private void writeRpcHeader(OutputStream outStream) throws IOException {
      DataOutputStream out = new DataOutputStream(new BufferedOutputStream(outStream));
      out.write(Server.HEADER.array()); 		// ① "hrpc", 4个字节, 写入dataLengthBuffer(4)
      out.write(Server.CURRENT_VERSION); 	// ② 1个字节, 写入rpcHeaderBuffer(2)的第1个字节
      authMethod.write(out); 				// ② 1个字节,写入rpcHeaderBuffer(2)的第2个字节
      out.flush();
    }
    
    /* Write the protocol header for each connection*/
    private void writeHeader() throws IOException {
      DataOutputBuffer buf = new DataOutputBuffer();
      header.write(buf);					// Write out the ConnectionHeader
      
      int bufLen = buf.getLength(); 			// Write out the payload length
      out.writeInt(bufLen);				// ③ int类型, 4个字节, 写入dataLengthBuffer(4)
      out.write(buf.getData(), 0, bufLen);	// ④ 显示长度, 长度为bufLength    -> ⑤ processHeader
    }

    public void sendParam(Call call) {
      DataOutputBuffer d = new DataOutputBuffer();
      d.writeInt(call.id);
      call.param.write(d);

      byte[] data = d.getData();
      int dataLength = d.getLength();
      out.writeInt(dataLength);      		// ⑥ first put the data length . int类型, 4个字节, 写入dataLengthBuffer(4)
      out.write(data, 0, dataLength);		// ⑦ write the data 显示长度, 长度为dataLenth
      out.flush();
    }  
{% endhighlight %}

![Connection flow1](https://n4tfqg.blu.livefilestore.com/y2pEqGMA7x4tY5rDQTrwndWmD8TYTqQj6WlbX64fKomQTK6XQRR5IPR3PNgJv-DTB5rPJk28rFZNqv6RS8hi2v6WRvQzkFnf1M2a9T4mhLVYZC0hDLWMogk41SF1j_Ea0qX/6-7%20Conn%20flow-1.png?psid=1)  
![Connection flow2](https://n4tfqg.blu.livefilestore.com/y2pDq30STSsxOaSrfecsB5Rc9T4_ra6WMRfN38jXYzpoBsoFmUdZqiLDd_gVbWOPujrhsZivLKQvu_u2O0BC_gVbBMmI9DOvEv_yFiz1DiZ636n5owhw5coknrSE9ps-_-b/6-7%20Conn%20flow-2.png?psid=1)  
![Connection flow3](https://n4tfqg.blu.livefilestore.com/y2pmOLZ5ue6ocm7katX_uQYdRFPh_K0D1AuzgL7azBlpFvQLh6wsZAUj4C6Pqo1y4CBjfMs0Ua1rRu0scOyVtrEUt0tk-IcHKV4C0t-io9cy7Qrt4E1o4jt4AyZen5pJNjG/6-7%20Conn%20flow-3.png?psid=1)  


####readAndProcess
{% highlight java %}
  private BlockingQueue<Call> callQueue; // queued calls

  public class Connection {

    // 接收调用数据的核心方法, 实现了如何从SocketChannel通道中读取数据 
    public int readAndProcess() throws IOException, InterruptedException {
      while (true) {
        /* Read at most one RPC. If the header is not read completely yet then iterate until we read first RPC or until there is no data left. */    
        int count = -1;
 		// ① 读取Client发送的writeRpcHeader的的HEADER: 从通道channel中读取字节, 加入到dataLengthBuffer字节缓冲区
 		// ③ 读取Client发送的writeHeader中ConnectionHeader的length
 		// ⑥ 读取Client发送的sendParam中Call的length
        if (dataLengthBuffer.remaining() > 0) { 
          count = channelRead(channel, dataLengthBuffer);       
          if (count < 0 || dataLengthBuffer.remaining() > 0)  // 读取不成功, 直接返回读取的字节数（读取失败可能返回0或-1） 
            return count;
        }
        // ConnectionHeader连接头的长度读取完毕后, 进入④, 下面②的代码不会执行因为rpcHeaderRead已经被置为true
        // ② 读取Rpc请求头(版本和授权方法). 客户端writeRpcHeader写入了HEADER, CURRENT_VERSION, authMethod
        if (!rpcHeaderRead) { //Every connection is expected to send the header.
          if (rpcHeaderBuffer == null) {
            rpcHeaderBuffer = ByteBuffer.allocate(2); // 2个字节
          }
          count = channelRead(channel, rpcHeaderBuffer); 
          if (count < 0 || rpcHeaderBuffer.remaining() > 0) {
            return count;
          }
          int version = rpcHeaderBuffer.get(0);
          byte[] method = new byte[] {rpcHeaderBuffer.get(1)};
 		  authMethod = AuthMethod.read(new DataInputStream(new ByteArrayInputStream(method)));

          dataLengthBuffer.flip();	//反转dataLengthBuffer缓冲区 
          if (!HEADER.equals(dataLengthBuffer) || version != CURRENT_VERSION) return -1;
 		  dataLengthBuffer.clear();	//清空dataLengthBuffer以便重用 
 		  rpcHeaderBuffer = null;
          rpcHeaderRead = true;	//设置读取rpdHeader完成
          continue;				// ③ 继续while循环的内容, 即再次读取数据到dataLengthBuffer中, 这次读取的是ConnectionHeader的length
        }

        // ④ ⑦ data还没收到数据, 构造data大小为前一次dataLengthBuffer中的内容,即数据的长度
        if (data == null) { 							//数据缓冲区为空(默认还没分配大小),则根据数据长度来分配数据缓冲区的大小
          dataLengthBuffer.flip();					//反转dataLengthBuffer缓冲区
          dataLength = dataLengthBuffer.getInt(); 	//读取数据长度信息, 以便分配data字节缓冲区 
          data = ByteBuffer.allocate(dataLength);  	//分配data数据缓冲区, 准备接收调用参数数 
        }
 	   	// ④ 读取ConnectionHeader的数据内容
 	   	// ⑦ 读取Call的数据内容
        count = channelRead(channel, data);			//从通道channel中读取字节到data字节缓冲区中 
        
        if (data.remaining() == 0) { 					//数据读取完毕, data缓冲区已经如期读满 
          dataLengthBuffer.clear();					//清空dataLengthBuffer  
          data.flip();								//反转data字节缓冲区, 准备从data缓冲区读取数据 
          boolean isHeaderRead = headerRead;
          if (useSasl) {
            saslReadAndProcess(data.array());
          } else {
            processOneRpc(data.array()); 			// 处理请求
          }
          data = null;								// 重置data字节缓冲区, 以备下一个连接到来时缓冲字节 
          if (!isHeaderRead) { // processHeader后设置headerRead=true, 并继续while循环. 下一次到这里(sendParam)时不再进入while循环了而是return
            continue;							// ⑥
          }
        } 
        return count;								// ⑨ 表示一次完整的RPC调用(三个方法)处理完成后返回. 
      }  // 如果有下一次的Connection请求, 继续调用Connection.readAndProcess()
    }
    
    // 该方法的调用者readAndProcess是个循环, 因此当Client发送writeHeader时, 进入else, 当Client发送sendParam时进入if
    private void processOneRpc(byte[] buf) throws IOException, InterruptedException {
      if (headerRead) {		// ⑧ 如果头信息已经读过了, 读取到的一定是RPC调用参数数据 
        processData(buf);		// 处理读取到的调用数据 
      } else {					// 如果头信息未读 
        processHeader(buf);	// ⑤ 读取版本号后面的连接头信息 
        headerRead = true;	// 设置连接头信息已经读取过. 同一个Connection的多次RPC调用,只调用一次processHeader,每次都处理processData 
      }
    }
    
    // Reads the connection header following version  writeHeader写入的ConnectionHeader在这里进行处理
    private void processHeader(byte[] buf) throws IOException {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(buf));
      header.readFields(in);  // 读取出ConnectionHeader	 
      try {
        String protocolClassName = header.getProtocol();  // 获取ConnectionHeader的protocol
        if (protocolClassName != null) {
          protocol = getProtocolClass(header.getProtocol(), conf); // 将连接头的协议接口类设置到Connection的protocol属性上
        }
      } catch (ClassNotFoundException cnfe) {}
      
      UserGroupInformation protocolUser = header.getUgi(); // 获取ConnectionHeader的ticket
      if (!useSasl) {
        user = protocolUser; // 设置到Connection的user属性上
        if (user != null)  user.setAuthenticationMethod(AuthMethod.SIMPLE.authenticationMethod); // 设置该User的authMethod
      }
    }
    
    //client.sendParam(call) 发送data(包括call.id和call.param)和dataLength
    //通过反序列化操作从网络字节流中冲重构调用参数数据对象, 并构造Server.Call对象, 
    //同时加入callQueue队列, 等待Server.Handler线程进行处理
    private void processData(byte[] buf) throws IOException, InterruptedException {
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buf));
      int id = dis.readInt();  	// try to read an id
      Writable param = ReflectionUtils.newInstance(paramClass, conf);//read param
      param.readFields(dis);        
        
      Call call = new Call(id, param, this); //封装成Call对象. 第三个参数为Connection就当前连接
      callQueue.put(call);  	// queue the call; maybe blocked here 将Call放入callQueue队列等待队列从中取出并执行RPC调用
      incRpcCount();  		// Increment the rpc count 增加RPC调用统计计数 
    }
}
{% endhighlight %}

processData()的参数byte[] buf为客户端发送的Call.id+Call.param(上面已经分析了将数据从SocketChannel读取到data缓冲区中,当缓冲区被读满后反转缓冲区就可以读取data缓冲区里的内容了). 根据buf构建了一个输入流, 从输入流中读取到客户端sendParam传送的call.id和call.param.  
由于param是可序列化的, paramClass在Server中我们分析过了(RPC.getServer()-->new Server时传入的),是RPC的Invocation.class.  
 
Reader线程在读取数据时, 要保留Connection的引用, 因为需要知道是哪个客户端写入数据. 这也是为什么Server.Call有属性Connection.  
在processData方法里的new Call(id, param, this)其中this值得是当前类即Connection的引用. 然后将新建的Call加入到队列中.  

callQueue队列的初始化在创建Server时被初始化为LinkedBlockingQueue<Call>, 上面的分析中在连接建立后客户端向服务器发送数据,交给Listener的Reader线程读取数据. Reader将Call放入callQueue. 有put的地方就有take, put()发生在Connection里, take()发生在Handler里  
因为在服务器启动的时候, 监听器, Reader[], Connection, Handler[]都会启动线程. 所以一旦有put, take就能取出Call对象进行RPC调用.  


###Handler
{% highlight java %}
  /** This is set to Call object before Handler invokes an RPC and reset after the call returns.
  * 和SERVER一样CurCall也是个ThreadLocal对象,确保了线程安全.*/
  private static final ThreadLocal<Call> CurCall = new ThreadLocal<Call>();
  /** Called for each call. */
  public abstract Writable call(Class<?> protocol,Writable param, long receiveTime)throws IOException;

  /** Handles queued calls . */
  private class Handler extends Thread {
    public void run() {
      SERVER.set(Server.this);		//设置当前处理线程的本地变量的拷贝 
      ByteArrayOutputStream buf = new ByteArrayOutputStream(INITIAL_RESP_BUF_SIZE); //存放响应信息的缓冲区10240 
      while (running) {
          final Call call = callQueue.take(); // pop the queue; maybe blocked here 出队操作, 获取到一个调用Server.Call 
          Writable value = null;	 	//call的返回值
          CurCall.set(call);			//设置当前线程本地变量拷贝的值为出队得到的一个call调用实例 

          // Make the call as the user via Subject.doAs, thus associating the call with the Subject
 		  //根据调用Server.Call关联的连接Server.Connection, 所对应的用户, 来执行IPC调用过程 
          if (call.connection.user == null) {
              value = call(call.connection.protocol, call.param, call.timestamp); //protocol为接口实现类, param封装了方法和参数
          } else {
              value = call.connection.user.doAs(new PrivilegedExceptionAction<Writable>() {
                  public Writable run() throws Exception {
                     return call(call.connection.protocol, call.param, call.timestamp); // make the call 执行调用 
                  }
              });
          }
          CurCall.set(null);	//当前Handler线程处理完成一个调用call, 回收当前线程的局部变量拷贝 
      }    
    }
  }
{% endhighlight %}

####Call.param历险记
Handler的run方法里调用了Server的抽象方法call(), 会调用到实现类RPC.Server.call(). 参数中的call变量从callQueue中取出队列的第一个元素进行处理

    call(call.connection.protocol, call.param, call.timestamp)
其中call.connection.protocol的值由Client.writeHeader()- > Server.Connection.readAndProcess() -> processHeader()赋值进来,   因为ConnectionHeader包括了protocol,ticket,authMethod.  call.param通过Client.Connection.sendParam() -> Server.Connection.readAndProcess() -> processData()完成.  

RPC.getServer() --> new RPC.Server()传入的Invocation.class在ipc.Server中对应了paramClass, 通过paramClass新建了Server.Call.   Handler取出的Server.Call又调用回RPC.Server.call(). 因为最开始传入的是Invocation.class, 所以在RPC.Server.call中可以将param转为Invocation.  

**RPC.Server**  
{% highlight java %}
    public Server(Object instance, Configuration conf, String bindAddress,  int port, 
              int numHandlers, boolean verbose, SecretManager secretManager) {
      super(bindAddress, port, Invocation.class, numHandlers, conf, classNameBase(instance.getClass().getName()), secretManager);
      this.instance = instance;
      this.verbose = verbose;
    }
{% endhighlight %}

**Server**  
{% highlight java %}
    private Class<? extends Writable> paramClass;   // class of call parameters
    protected Server(String bindAddress, int port, Class<? extends Writable> paramClass, int handlerCount, ...) {
      this.bindAddress = bindAddress;
      this.conf = conf;
      this.port = port;
      this.paramClass = paramClass;
    }
{% endhighlight %}

**Server.Connection**  
{% highlight java %}
    private void processData(byte[] buf) {
      Writable param = ReflectionUtils.newInstance(paramClass, conf);//read param      
      Call call = new Call(id, param, this); //封装成Call对象. 第三个参数为Connection就当前连接
      callQueue.put(call);
    }
{% endhighlight %}

**Handler**  
{% highlight java %}
    public void run() {
 		  Call call = callQueue.take()
 		  call(call.connection.protocol, call.param, call.timestamp)
    }
{% endhighlight %}

**RPC.Server.call()**  
{% highlight java %}
    public Writable call(Class<?> protocol, Writable param, long receivedTime) throws IOException {
        Invocation call = (Invocation)param; //将参数转换为Invocation对象
        Method method = protocol.getMethod(call.getMethodName(), call.getParameterClasses()); //取得Invocation中的方法名和参数
        method.setAccessible(true);
        Object value = method.invoke(instance, call.getParameters()); //反射调用: instance为接口的实现类.调用instance的method,参数为call.parameters
        return new ObjectWritable(method.getReturnType(), value); //将RPC调用的返回结果封装成ObjectWritable类型
    }
{% endhighlight %}

Handler线程主要的任务是：真正地实现了处理来自客户端的调用(反射调用到协议实现类的方法), 并设置每个相关调用的响应setupResponse().  
Handler将Call调用的id, 状态, 返回值最终设置到call调用的response缓冲区. 具体的响应(服务器向客户端写入数据)交给Responder处理.  
因为Call对象始终贯穿在整个流程中, 所以Responder获取到Call对象,能够从Call中获取到数据,并负责向客户端写入数据.  

Handler的run方法在调用call之后的处理:  
{% highlight java %}
          synchronized (call.connection.responseQueue) {
            // setupResponse() needs to be sync'ed together with responder.doResponse() since setupResponse may use
            // SASL to encrypt response data and SASL enforces its own message ordering.
 			//处理当前获取到的调用的响应,将响应数据设置到Call的response缓冲区里 
            setupResponse(buf, call, (error == null) ? Status.SUCCESS : Status.ERROR, value, errorClass, error);
            // Discard the large buf and reset it back to smaller size to freeup heap
            if (buf.size() > maxRespSize) {
              buf = new ByteArrayOutputStream(INITIAL_RESP_BUF_SIZE);
            }
 			//将调用call(已经将response设置到call中,后面就能从call中读取出数据)加入到响应队列中, 等待客户端读取响应信息  
            responder.doRespond(call);
          }
{% endhighlight %}

####Server.setupResponse()
{% highlight java %}
  /** Setup response for the IPC Call. 设置RPC调用的响应信息   与客户端读取数据互相对应,这里写什么,客户端就读取什么.
   * @param response buffer to serialize the response into 缓冲区用来序列化响应信息, 响应要发送给客户端
   * @param call to which we are setting up the response 调用对象
   * @param status of the IPC call 调用状态,成功?失败?
   * @param rv return value for the IPC Call, if the call was successful 如果调用成功返回方法的执行结果 */
  private void setupResponse(ByteArrayOutputStream response, Call call, Status status, Writable rv, String errorClass, String error) {
    response.reset();
    DataOutputStream out = new DataOutputStream(response);
    out.writeInt(call.id);              	// write call id 在Client端读取数据, 根据call.id从Call池中获取调用对象,所以要写入call.id
    out.writeInt(status.state);         	// write status 调用状态, 客户端根据该状态来进一步做判断
    if (status == Status.SUCCESS) {		//调用成功设置返回值, 否则设置异常信息
      rv.write(out);					// write return value
    } else {
      WritableUtils.writeString(out, errorClass);
      WritableUtils.writeString(out, error);
    }
    if (call.connection.useWrap) {
      wrapWithSasl(response, call);
    }
    call.setResponse(ByteBuffer.wrap(response.toByteArray())); //将response设置到Call的response缓冲区中
  }
{% endhighlight %}

Server.setupResponse()发送的数据(实际上此时还没发送)对应的接收方是Client.Connection的receiveResponse()  
{% highlight java %}
    private void receiveResponse() {
        touch();
        int id = in.readInt();                    // try to read an id
        Call call = calls.get(id);

        int state = in.readInt();     			// read call status
        if (state == Status.SUCCESS.state) {
          Writable value = ReflectionUtils.newInstance(valueClass, conf); // RPC.ClientCache传入的是ObjectWritable.class
          value.readFields(in);                 // read value
          call.setValue(value);
          calls.remove(id);
        } else if (state == Status.ERROR.state) {
          call.setException(new RemoteException(WritableUtils.readString(in),WritableUtils.readString(in)));
          calls.remove(id);
        } else if (state == Status.FATAL.state) { 	// Close the connection
          markClosed(new RemoteException(WritableUtils.readString(in), WritableUtils.readString(in)));
        }
    }
{% endhighlight %}

但是请注意, setResponse并没有发送数据给客户端, 该方法仅起到了一个中介的作用, 将要写入客户端的数据通过DataOutputStream设置到call.response中, Server.Call的resopnse属性是ByteBuffer类型. 注意和前面Client.Connection.sendParam的方式不同,sendParam是写到缓冲区DataOutputBuffer, 再读取缓冲区的内容输出到DataOutputStream就可以向Server发送数据了. 其实看IO流是否发送/接收数据, 关键看输出流/输入流是否真正工作, 上面的setupResponse的DataOutputStream是一个方法内的变量, 不是作用在Server端的, 仅提供给ByteArrayOutputStream response使用, 所以是不会发送数据的. 而Client.Connection.sendParam的out变量是通过Socket获取到的DataOutputStream. 当向该对象写入数据是可以发送数据的.  

![6-8 Responder.doRespond](https://n4tfqg.blu.livefilestore.com/y2pSGzj-71dQEH7m2LZutyq0rTTqS4dVpKQ3Hx-TAHTT3Jmo12RkNO64cJgye0pkC2qU1d1St4BJpHyNWiBX2iWCtLWpg1c-aj-8AdFUZhSvO6BDfEQivCFqz5S5j1fAWx5/6-8%20Responder.doRespond.png?psid=1)  

因为call在上一步setupResponse将需要返回给客户端的信息设置到call对象的response, 下一步的操作就可以从call中取出response信息进行处理. 
处理方式: 将Call调用对象加入到responseQueue响应队列中, 由Responder线程来完成, Responder响应线程类实现了发送RPC响应到客户端.

    responder.doRespond(call);


###Responder 
####responder.doRespond(call)
响应队列responseQueue是Connection的属性LinkedList<Call> responseQueue. 访问方式通过call.connection.responseQueue. 需要进行同步.  
Responder线程的任务是: 处理响应队列responseQueue中的全部调用Call, 对应的响应数据.  
responseQueue响应队列, 可以理解为某个通道上调用的集合(calls)所对应的待处理响应数据的队列.  

Handler通过调用Responder.doRespond()将处理完的结果(call.response)交给Responder. 为什么不在Handler处理完IPC请求(call)后, 就将结果发送给Client? 一般情况下RPC调用使用Handler的线程执行被调用的过程(call->method.invoke). 也就是说Handler所在的线程其实是共享资源, 对共享资源占用的时间越短越好, 另一方面, 网络通信的时间是不确定的, 如果客户端比较忙的话, 响应时间会比较长(相对于RPC调用). 这样如果在RPC调用完成后如果马上将结果发送给Client, 就可能需要等待客户端在不忙的情况下才能接收. 所以Hadoop的RPC机制引入了Responder应答器来发送IPC的处理结果.  
![6-9 Multi RPC Call](https://n4tfqg.blu.livefilestore.com/y2phnBucdLPxpOBgCEZI5o6gTg6dqtLxxmHimJ35KMAxvCbwbcMlnU8D4tCIw2BCKDx-MbgAWrMrK8RfWvoqgqdOqS3eya3MMyIN_X6F3YpoEXZ1fTtFSlgxwRBR7F3SzyK/6-9%20Multi%20RPC%20Call.png?psid=1)  


在将应答(Call)放入队列后, 做了一个特殊的处理, 如果IPC连接的responseQueue只有一个元素, 立即调用processResponse()向Client发送处理结果,其中第二个参数inHandler = true, 表示processResponse的处理线程仍然是Handler, 而不是交由Response线程处理. 这是一个提高服务器性能的优化, 当应答队列responseQueue只有一个元素的时候, 表明对应的IPC连接比较空闲, 这时候直接调用processResponse()发送应答, 可以避免从Handler的处理线程到Responder处理线程的切换开销.  
{% highlight java %}
    // Enqueue a response from the application. 从队列中弹出response
    //当完成一个Call调用后, Handler会将调用的Call加入到响应队列的末尾, Call包含了服务端对客户端的响应
    void doRespond(Call call) throws IOException {
      synchronized (call.connection.responseQueue) {
        call.connection.responseQueue.addLast(call);				//将执行完成的调用加入队列, 准备响应客户端 
        if (call.connection.responseQueue.size() == 1) {				//如果队列中只有一个调用, 直接进行处理 
          processResponse(call.connection.responseQueue, true); 	
        }
      }
    } 
{% endhighlight %}

前面的Listener线程的run()监听了OP_ACCEPT, Listener的内部类Reader线程的run()监听了OP_READ, 这里介绍的Responder线程的run()监听的是OP_WRITE事件. 因为Responder负责向Client写入处理结果数据.  
Listener监听的OP_ACCEPT的注册发生在new Listener的时候, Reader监听的OP_READ的注册发生在接受OP_ACCEPT事件后的doAccept()里.  
那么Responder监听的OP_WRITE的注册来自于哪里? ->processResponse inandler=true.  
处理响应的目标是将Call中的response(ByteBuffer) 写到通道中. 这样客户端就能从Socket连接中根据输入流取得数据.  
(Client采用阻塞IO只使用了Socket对象发送和接收对象, Server端采用NIO, 使用的对象有ServerSocketChannel, SocketChannel)  
{% highlight java %}
    // Processes one response. Returns true if there are no more pending data for this channel. 处理一个通道上调用的响应数据,如果该通道空闲返回true  
    private boolean processResponse(LinkedList<Call> responseQueue, boolean inHandler) throws IOException {
      boolean error = true;
      boolean done = false; 	// there is more data for this channel.当前通道channel有更多的数据待读取 
      int numElements = 0;
      Call call = null;
      try {
        synchronized (responseQueue) { 
          numElements = responseQueue.size(); 	//取得队列的个数,如果队列中有数据,Responder负责取出队列第一个元素开始工作
          if (numElements == 0) {				// If there are no items for this channel, then we are done 如果该通道channel空闲, 处理响应完成
            error = false;
            return true;    					// no more data for this channel. 当前通道上没有数据返回true 
          }
          call = responseQueue.removeFirst();				// ① Extract the first call 从队列中取出第一个调用call 
          SocketChannel channel = call.connection.channel;		// ② 获取该调用对应的通道channel(SocketChannel在Connection中) 
          // Send as much data as we can in the non-blocking fashion
 		  //将call.response缓冲区的数据(setupResponse中设置好了)写到SocketChannel, 这个SocketChannel就是客户端和服务器进行通信的通道
 		  //客户端根据和服务器建立的Socket连接,得到输入流就能取出数据(采用输入流是阻塞方式读取,并不是从通道中读取).
 		  //客户端和服务器建立的Socket连接, 以及服务器接受客户端建立的SocketChannel通道保证了数据的交互在同一个Connection里
          int numBytes = channelWrite(channel, call.response);	// ③ 向通道channel中写入响应信息（响应信息位于call.response字节缓冲区中） 
          if (numBytes < 0) {								//如果写入字节数为0, 说明已经没有字节可写, 返回 
            return true;
          }
          if (!call.response.hasRemaining()) {		// ④ 如果call.response字节缓冲区中没有响应字节数据,说明已经全部写入到相关联的通道中 
            call.connection.decRpcCount();		//该调用call对应的RPC连接计数减1 
            if (numElements == 1) {    			// last call fully processes. 最后一个调用已经处理完成 
              done = true;             		// no more data for this channel. 该通道channel没有更多的数据 
            } else {
              done = false;            		// more calls pending to be sent. 还存在尚未处理的调用, 要向通道发送数据 
            }
          } else { 							// ⑤ 如果call.response字节缓冲区中还存在未被写入通道的响应字节数据
            // If we were unable to write the entire response out, then insert in Selector queue.   		 
 			//如果不能够将全部的响应字节数据写入到通道中, 需要暂时插入到Selector选择其队列中
 			//响应的数据如果不能一次性由channelWrite(channel,call.response)写入到通道中,就要启用Responder线程的writeSelector来操作
            call.connection.responseQueue.addFirst(call);  
            
 			// true=通过Handler(Connection)调用doRespond再调用到该方法, false=Responder.run()-->doAsyncWrite
            if (inHandler) {	//对调用call进行处理（该调用的响应还没有进行处理） 
              call.timestamp = System.currentTimeMillis(); // set the serve time when the response has to be sent later
              incPending();	//增加未被处理响应信息的调用计数 
              try {
                // Wakeup the thread blocked on select, only then can the call to channel.register() complete. 唤醒阻塞在该通道writeSelector上的线程 
                writeSelector.wakeup();
                channel.register(writeSelector, SelectionKey.OP_WRITE, call);	//调用call注册通道writeSelector. 最后一个参数相当于key.attach(call)
              } catch (ClosedChannelException e) {
                done = true; 	//Its ok. channel might be closed else where.
              } finally {
                decPending(); // 经过上面处理, 不管在处理过程中正常处理, 或是发生通道已关闭异常, 最后都将设置该调用完成, 更新计数 
              }
            }
          }
          error = false;   		// everything went off well
        }
      } finally {
        if (error && call != null) {
          done = true;   		// error. no more data for this channel.
          closeConnection(call.connection);
        }
      }
      return done;
    }
{% endhighlight %}

processResponse的处理是:  

	① 	从responseQueue取出队列的第一个元素Call, 
	② 	获得该Call对应的Connection的SocketChannel(Server通过该Socket通道和Client进行)
	③ 	将call的response响应(在setupResponse中设置过了)写入到SocketChannel中, 
　　	这个过程实际上就是channel.write(buffer)的过程:将缓冲区的数据写入到通道中. 只要将数据写入到通道中, 客户端就能取到数据.
	④ 	如果一次性全部写完, buffer.hasRemaining=false, numBytes>0
	⑤ 	如果不能一次写完, 首先要将该Call放入队列的头部, 在下一次调用时优先处理这个还没有完成的Call.
　　	这样下一次调用的时候再从①开始, 做到③, 继续将剩余的缓冲区的数据写到通道中.


往Responder的通道注册writeSelector的OP_WRITE事件, 类似于在Listener的doAccept中注册readSelector的OP_READ事件
**Listener:**  
{% highlight java %}
    void doAccept(SelectionKey key) {
        try {
          reader.startAdd();											//激活Reader.readSelector, 设置adding为true 
          SelectionKey readKey = reader.registerChannel(channel); 			// ③ 将读事件设置成兴趣事件 
          c = new Connection(readKey, channel, System.currentTimeMillis());	//创建一个连接对象 
          readKey.attach(c);											//将connection对象注入readKey        
        } finally {
          reader.finishAdd();  //设置adding为false,采用notify()唤醒一个reader, 初始化Listener时启动的每个reader都使用了wait()方法等待
        }
    }
{% endhighlight %}

**Reader:**  
{% highlight java %}
      private volatile boolean adding = false;
    
      public synchronized SelectionKey registerChannel(SocketChannel channel) throws IOException {
    	  return channel.register(readSelector, SelectionKey.OP_READ);
      }
      public void startAdd() 
       	adding = true;
        readSelector.wakeup();
      }
      public synchronized void finishAdd() {
        adding = false;
        this.notify();        
      }

      public void run() {
          while (running) {
              readSelector.select();
              while (adding) {
                this.wait(1000);
              }              
              Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
              while (iter.hasNext()) {
                SelectionKey key = iter.next();
                iter.remove();
                if (key.isValid() && key.isReadable()) 
                    doRead(key);
                key = null;
              }
          }
      }
{% endhighlight %}

**Responder**  
{% highlight java %}
    private int pending = 0;
    
    private boolean processResponse(LinkedList<Call> responseQueue, boolean inHandler) throws IOException {
     	if (inHandler) {
            incPending();	//增加未被处理响应信息的调用计数 
            try {
              writeSelector.wakeup();
              channel.register(writeSelector, SelectionKey.OP_WRITE, call);	//调用call注册通道writeSelector. 最后一个参数相当于key.attach(call)
            } finally {
              decPending(); // 经过上面处理, 不管在处理过程中正常处理, 或是发生通道已关闭异常, 最后都将设置该调用完成, 更新计数 
            }
      }
    }
    
    public void run() {
      while (running) {
          waitPending();     // If a channel is being registered, wait.
          writeSelector.select(PURGE_INTERVAL);
          Iterator<SelectionKey> iter = writeSelector.selectedKeys().iterator();
          while (iter.hasNext()) {
            SelectionKey key = iter.next();
            iter.remove();
              if (key.isValid() && key.isWritable())
                  doAsyncWrite(key);
          }
 	  }
    }
    private synchronized void incPending() {   // call waiting to be enqueued.
      pending++;
    }
    private synchronized void decPending() { // call done enqueueing.
      pending--;
      notify();
    }
    private synchronized void waitPending() throws InterruptedException {
      while (pending > 0) 
        wait();
    }
{% endhighlight %}

![6-10 Reader Responder](https://n4tfqg.blu.livefilestore.com/y2pfWi-HTlb3uVV9HZRSCgyrXJKZ12ou0HoOFXT8kikTC22LCOv5mZFhabunnMGwAQkA6dx_7GnEKgC_kGP-0e2_5O2xNAod7dXMjPxhAIZQYHd24hE2bhAOAqIlQXZojbE/6-10%20Reader%20Responder.png?psid=1)  

####Responder.run()
{% highlight java %}
  // Sends responses of RPC back to clients.
  private class Responder extends Thread {
    private Selector writeSelector;
    private int pending;         // connections waiting to register
    
    Responder() throws IOException {
      this.setName("IPC Server Responder");
      this.setDaemon(true);
      writeSelector = Selector.open(); // create a selector
      pending = 0;
    }

    public void run() {
      SERVER.set(Server.this);  
      while (running) {
          waitPending();    					// If a channel is being registered, wait. 等待一个通道中, 接收到来的调用进行注册 
          writeSelector.select(900000); //设置超时时限  15mins
          Iterator<SelectionKey> iter = writeSelector.selectedKeys().iterator();
          while (iter.hasNext()) {
            SelectionKey key = iter.next();
            iter.remove();
            if (key.isValid() && key.isWritable()) { 	//如果合法, 并且通道可写 
               doAsyncWrite(key); 			//执行异步写操作, 向通道中写入调用执行的响应数据 
            }
          }          
      }
      LOG.info("Stopping " + this.getName()); // running=false,退出while循环表示Responder线程停止工作
    }

 	// 当某个通道上可写的时候, 可以执行异步写响应数据的操作 
    private void doAsyncWrite(SelectionKey key) throws IOException {
      Call call = (Call)key.attachment();
      if (call == null) return;
      if (key.channel() != call.connection.channel) throw new IOException("doAsyncWrite: bad channel");
      synchronized(call.connection.responseQueue) {
        if (processResponse(call.connection.responseQueue, false)) {
            key.interestOps(0); //取消注册事件即OP_WRITE, 因为processResponse返回true, 缓冲区中没有可写的数据要发送到通道上
        }
      }
    }
{% endhighlight %}

通过线程执行可以看到, 调用的响应数据(Responder)的处理, 是在服务器运行过程中处理的, 而且分为两种情况:  

	1、如果所选择的通道上, 已经注册的调用是合法的, 并且通道可写, 会直接将调用的相应数据写入到通道, 等待客户端读取;(run上半部分)
	2、如果某些调用超过了指定的时限而一直未被处理, 这些调用被视为过期, 服务器不会再为这些调用处理, 而是直接清除掉(run下半部分)

run的while循环的下半分是清除过期的调用:  
{% highlight java %}
    public void run() {
      long lastPurgeTime = 0;   				// last check for old calls. 最后一次清除过期调用的时间  
      while (running) {

 		  // If there were some calls that have not been sent out for a long time, discard them. 如果存在一些一直没有被发送出去的调用
     	  //时间限制为lastPurgeTime + PURGE_INTERVAL, 则这些调用被视为过期调用, 进行清除
          long now = System.currentTimeMillis();
          if (now < lastPurgeTime + PURGE_INTERVAL) {
            continue;
          }
          lastPurgeTime = now;
          LOG.debug("Checking for old call responses.");
          ArrayList<Call> calls;
          // get the list of channels from list of keys.
          synchronized (writeSelector.keys()) {
            calls = new ArrayList<Call>(writeSelector.keys().size());
            iter = writeSelector.keys().iterator();
            while (iter.hasNext()) {
              SelectionKey key = iter.next();
              Call call = (Call)key.attachment();
              if (call != null && key.channel() == call.connection.channel) { 
                calls.add(call);
              }
            }
          }
          for(Call call : calls) {
              doPurge(call, now); // 执行清除 
          }
      }
  	}

    // Remove calls that have been pending in the responseQueue for a long time. 如果未被处理响应的调用在队列中滞留超过指定时限, 要定时清除掉 
    private void doPurge(Call call, long now) throws IOException {
      LinkedList<Call> responseQueue = call.connection.responseQueue;
      synchronized (responseQueue) {
        Iterator<Call> iter = responseQueue.listIterator(0);
        while (iter.hasNext()) {
          call = iter.next();
          if (now > call.timestamp + PURGE_INTERVAL) {
            closeConnection(call.connection);
            break;
          }
        }
      }
    }
  }
{% endhighlight %}



















