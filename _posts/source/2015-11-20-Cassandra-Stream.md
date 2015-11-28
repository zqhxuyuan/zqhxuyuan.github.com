---
layout: post
title: Cassandra源码分析之Stream
category: Source
tags: BigData
keywords: 
description: 
---

## sstableloader流程

BulkLoader主方法创建SSTableLoader,调用stream开始流式传输:  
```java
    //Future操作,非阻塞的. 把长时间执行的任务封装在Future里, 程序主逻辑继续往下执行, 通过future.get获取结果
    StreamResultFuture future = loader.stream(options.ignores);
    //如果没有获取到结果, 会一直阻塞下去, 直到任务完成, 才退出
    future.get();
```

SSTableLoader.stream方法返回Future才能让调用者在Future上调用get.  
```java
    public StreamResultFuture stream(Set<InetAddress> toIgnore, StreamEventHandler... listeners) {
        //初始化, 客户端为BulkLoader创建的ExternalClient. 
        client.init(keyspace);
        //构造一个流传输的执行计划. 流可以看做是流动,或者流式
        StreamPlan plan = new StreamPlan("Bulk Load").connectionFactory(client.getConnectionFactory());
        //在初始化之后, 能得到目标集群每个节点对一个的TokenRange集合. 因为一个节点有256个vnodes,所以一个节点会有很多个Token!
        Map<InetAddress, Collection<Range<Token>>> endpointToRanges = client.getEndpointToRangesMap();

        //打开sstables... 会生成streamingDetails, 下面进一步处理streamingDetails, 转换结构
        openSSTables(endpointToRanges);
        //上面的openSSTables是以一个个的SSTable为视觉, 现在回到stream主方法,需要以目标节点为视觉
        for (Map.Entry<InetAddress, Collection<Range<Token>>> entry : endpointToRanges.entrySet()) {
            InetAddress remote = entry.getKey();
            List<StreamSession.SSTableStreamingSections> endpointDetails = new LinkedList<>();
            // transferSSTables assumes references have been acquired.  streamingDetails是从openSSTables中得到的
            for (StreamSession.SSTableStreamingSections details : streamingDetails.get(remote)) {
                endpointDetails.add(details);
            }
            //标记remote endpoint节点需要处理这些stream sections
            plan.transferFiles(remote, endpointDetails);
        }
        //监听器,比如进度条ProgressIndicator
        plan.listeners(this, listeners);

        //真正开始执行StreamPlan
        return plan.execute();
    }
```

先调用StreamPlan的transferFiles, 等所有endpoints都遍历完才开始execute. 在transferFiles会准备一些execute必备的数据比如sessions.  
```java
    public StreamPlan transferFiles(InetAddress to, Collection<StreamSession.SSTableStreamingSections> sstableDetails) {
        //节点to对应的Session如果已经存在则直接获取,没有就创建
        StreamSession session = getOrCreateSession(to, to);
        //为这个Session添加任务: 要传输的sstable文件
        session.addTransferFiles(sstableDetails);
        return this;
    }

    private StreamSession getOrCreateSession(InetAddress peer, InetAddress preferred) {
        //sessions是怎么来的? 只有在这个方法里put进去的. 所以调用该方法,如果不在sessions就会new一个并放进来
        //peer有可能是from节点,比如目标节点/接收数据的节点. peer也可能是to节点,目标节点,要传输到这个目标节点.
        //那么from和to就有可能是同一个节点.比如执行sstable命令的节点是源,则接收数据to/请求数据from的节点是目标节点
        StreamSession session = sessions.get(peer);
        if (session == null) {
            session = new StreamSession(peer, preferred, connectionFactory);
            sessions.put(peer, session);
        }
        return session;
    }    
```

StreamPlan.execute返回的是一个全局唯一的StreamResultFuture,基于Future.  
```java
    public StreamResultFuture execute() {
        //只有一个StreamPlan,但是有好多个StreamSession. 要开始一起开始吧
        return StreamResultFuture.init(planId, description, sessions.values(), handlers);
    }
```

初始化StreamResultFuture会创建StreamResultFuture并注册到StreamManager,然后把它传递给所有StreamSession的初始化方法, 最后启动每个StreamSession:  
```java
    //初始化异步返回结果器. 一个StreamPlan只有一个StreamResultFuture,有多个StreamSessions, 所有的StreamSessions共用一个StreamResultFuture
    //因为一次Stream只需要最后的一个结果来表示所有(节点)的StreamSession是否都已经完成. 一个StreamSession对应一个节点的传输.
    static StreamResultFuture init(UUID planId, String description, Collection<StreamSession> sessions, Collection<StreamEventHandler> listeners) {
        StreamResultFuture future = createAndRegister(planId, description, sessions);
        if (listeners != null) {
            //给异步执行结果添加监听器
            for (StreamEventHandler listener : listeners)
                future.addEventListener(listener);
        }

        logger.info("[Stream #{}] Executing streaming plan for {}", planId,  description);
        // start sessions
        for (final StreamSession session : sessions) {
            logger.info("[Stream #{}] Beginning stream session with {}", planId, session.peer);
            //StreamPlan的execute会启动同一个StreamPlan的所有StreamSession. 最后实际执行的还是要交给StreamSession
            session.init(future);
            //启动每一个节点的StreamSession任务
            session.start();
        }
        //为什么要返回Future:对于异步执行的任务需要返回Future,这样调用者才能使用future.get来获得结果
        return future;
    }
```

由于在execute前已经transferFiles,所以每个StreamSession的transfers都是有数据的,当然也可能是requests. 然后用线程池启动任务 
```java
    public void start() {
        //请求或者传输必选其一,否则说明这个Session已经完成了
        if (requests.isEmpty() && transfers.isEmpty()){
            closeSession(State.COMPLETE);
            return;
        }
        streamExecutor.execute(new Runnable() {
            public void run() {
                //准备好ConnectionHandler
                handler.initiate();
                //初始化完毕
                onInitializationComplete();
            }
        });
    }
```

初始化ConnectionHandler创建输入和输出的消息处理器. handler管理这两个线程.  
```java
    public void initiate() throws IOException {
        Socket incomingSocket = session.createConnection();
        incoming.start(incomingSocket, StreamMessage.CURRENT_VERSION);
        incoming.sendInitMessage(incomingSocket, true);

        Socket outgoingSocket = session.createConnection();
        outgoing.start(outgoingSocket, StreamMessage.CURRENT_VERSION);
        outgoing.sendInitMessage(outgoingSocket, false);
    }
```

输入和输出MessageHandler都继承MessageHandler抽象线程类,初始化时都发送InitMessage:    
```java
        public void sendInitMessage(Socket socket, boolean isForOutgoing) throws IOException {
            //创建初始化消息, 并转化为ByteBuffer, 由WriteChannel发送出去(即写入到WriteChannel中)
            //WriteChannel是由Socket创建的, 表示要写入到Socket这个地址创建的写入通道中
            StreamInitMessage message = new StreamInitMessage(FBUtilities.getBroadcastAddress(), session.planId(), session.description(), isForOutgoing);
            ByteBuffer messageBuf = message.createMessage(false, protocolVersion);
            while (messageBuf.hasRemaining())
                getWriteChannel(socket).write(messageBuf);
        }
```

初始化完毕StreamSession.start开始发送PREPARE准备消息:  
```java
    public void onInitializationComplete() {
        // send prepare message
        state(State.PREPARING);
        PrepareMessage prepare = new PrepareMessage();

        //消息中附带了requests请求或者传输任务transfers(任务一开始只是Summary)
        prepare.requests.addAll(requests);
        for (StreamTransferTask task : transfers.values())
            prepare.summaries.add(task.getSummary());

        //发送消息
        handler.sendMessage(prepare);

        // if we don't need to prepare for receiving stream, start sending files immediately
        if (requests.isEmpty())
            startStreamingFiles();
    }
```

发送消息放到OutgoingMessageHandler.messageQueue队列中. 与此同时输出线程从队列中获取消息并序列化消息到out写入通道中:  
StreamMessage是消息的抽象类,各类消息需要有自己的序列化实现器,因为不同类型的消息里面的内容是不一样的.  
```java
    //发送消息, 需要序列化消息
    private void sendMessage(WritableByteChannel out, StreamMessage message) {
        StreamMessage.serialize(message, out, protocolVersion, session);
    }
```

现在假设往out发送了PrepareMessage消息, 与此同时ConnectionHandler的输入线程IncomingMessageHandler收到了这条消息进行反序列化:  
```java
    public void run() {
        ReadableByteChannel in = getReadChannel(socket);
        while (!isClosed()) {
            // receive message
            StreamMessage message = StreamMessage.deserialize(in, protocolVersion, session);
            // Might be null if there is an error during streaming (see FileMessage.deserialize). It's ok to ignore here since we'll have asked for a retry.
            if (message != null) {
                session.messageReceived(message);
            }
        }
    }
```

StreamSession负责处理消息,如果是PrepareMessage,从中获取出附带的requests和transfers调用prepare方法:  
```java
    public void messageReceived(StreamMessage message) {
        switch (message.type) {
            case PREPARE:
                PrepareMessage msg = (PrepareMessage) message;
                prepare(msg.requests, msg.summaries);
                break;
            case FILE:
                receive((IncomingFileMessage) message);
                break;
            case RECEIVED:
                ReceivedMessage received = (ReceivedMessage) message;
                received(received.cfId, received.sequenceNumber);
                break;
        }
    }
```

PREPARE后就上开始传输文件了:  
```java
    private void startStreamingFiles() {
        streamResult.handleSessionPrepared(this);

        state(State.STREAMING);
        for (StreamTransferTask task : transfers.values()) {
            Collection<OutgoingFileMessage> messages = task.getFileMessages();
            if (messages.size() > 0)
                handler.sendMessages(messages);
            else
                taskCompleted(task); // there is no file to send
        }
    }
```

在StreamPlan的transferFiless中会调用StreamSession.addTransferFiles将要传输的文件加入到StreamTransferTask:  
```java
    //为Session添加要传输的文件列表, 添加到TransferTask中
    public void addTransferFiles(Collection<SSTableStreamingSections> sstableDetails) {
        Iterator<SSTableStreamingSections> iter = sstableDetails.iterator();
        while (iter.hasNext()) {
            SSTableStreamingSections details = iter.next();
            //每张表应该都有一个唯一的ID, 而且ID是不会变的吧对于同一张表而言
            UUID cfId = details.sstable.metadata.cfId;
            StreamTransferTask task = transfers.get(cfId);
            if (task == null) {
                task = new StreamTransferTask(this, cfId);
                transfers.put(cfId, task);
            }
            //针对同一张表,只会有一个TransferTask.但是这个Task的tables文件会很多
            task.addTransferFile(details.sstable, details.estimatedKeys, details.sections);
            //SSTableStreamingSections阅后即焚
            iter.remove();
        }
    }
```

传输文件的类型是OutgoingFileMessage, 所以上面startStreamingFiles开始传输的消息是Collection<OutgoingFileMessage>,  
因为一个Task可以调用多次addTransferFile就有多个要传输的文件(上面的cfId是CF表的编号,则sstableloader一次一个表就只有一个StreamTransferTask了):     
```java
    public synchronized void addTransferFile(SSTableReader sstable, long estimatedKeys, List<Pair<Long, Long>> sections) {
        //每一个要传输的文件都包装成输出文件消息, 序列号可以表示文件编号,因为调用一次就增加1. 其他信息sstable,sections都是从一开始沿袭过来的.
        OutgoingFileMessage message = new OutgoingFileMessage(sstable, sequenceNumber.getAndIncrement(), estimatedKeys, sections);
        files.put(message.header.sequenceNumber, message);
    }
```

OutgoingFileMessage的类型是FILE,对应messageReceived的会将消息转换为IncommingFileMessage并调用receive:  
```java
    public OutgoingFileMessage(SSTableReader sstable, int sequenceNumber, long estimatedKeys, List<Pair<Long, Long>> sections) {
        super(Type.FILE);
        this.sstable = sstable;
    }
```

实际上IncommingFileMessage的StreamMessage也是FILE. 这样Incomming和Outgoing各司其职: Outgoing输出负责序列化,Incomming输入负责反序列化.  

+ 输出消息OutgoingFileMessage的sstable是SSTableReader, 通过封装成StreamWriter输出.  
+ 读取消息IncomingFileMessage通过构造StreamReader读取输入流`reader.read(in)`最终形成SSTableWriter.  
+ `SSTableReader`和`SSTableWriter`均继承`SSTable`,用于读写SSTable文件, 但是`StreamReader`和`StreamWriter`提供的是流的读写/传输. 

```java
    //对输出消息进行序列化(output, write, serialize)
    public static Serializer<OutgoingFileMessage> serializer = new Serializer<OutgoingFileMessage>() {
        public void serialize(OutgoingFileMessage message, WritableByteChannel out, int version, StreamSession session) throws IOException {
            DataOutput output = new DataOutputStream(Channels.newOutputStream(out));
            FileMessageHeader.serializer.serialize(message.header, output, version);

            //包装在OutgoingFileMessage里的sstable是SSTableReader, 最早是在SSTableLoader:SSTableReader.openForBatch打开的.  
            final SSTableReader reader = message.sstable;
            //转换成StreamWriter, 为什么不直接是SSTableWriter? 因为要进行序列化和反序列化,用字节流形式即StreamWriter更快.  
            StreamWriter writer = message.header.compressionInfo == null ? new StreamWriter(reader, message.header.sections, session) :
                    new CompressedStreamWriter(reader, message.header.sections, message.header.compressionInfo, session);
            writer.write(out);
            session.fileSent(message.header);
        }
    };

    //读取输入消息反序列化(input, read, deserialize)
    public static Serializer<IncomingFileMessage> serializer = new Serializer<IncomingFileMessage>() {
        public IncomingFileMessage deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException {
            DataInputStream input = new DataInputStream(Channels.newInputStream(in));
            FileMessageHeader header = FileMessageHeader.serializer.deserialize(input, version);
            StreamReader reader = header.compressionInfo == null ? new StreamReader(header, session) : new CompressedStreamReader(header, session);
            return new IncomingFileMessage(reader.read(in), header);
        }
    };
```

|FileMessage|SSTable|Stream|IN/OUT|SER/DESER|read/write|
|-----------|-------|------|------|---------|----------|
|OutgoingFileMessage|SSTableReader|StreamWriter|Output|serialize|writer.write(out)
|IncomingFileMessage|SSTableWriter|StreamReader|Input|deserialize|reader.read(in)

StreamSession的receive将IncomingFileMessage转换为ReceivedMessage:   
```java
    public void receive(IncomingFileMessage message) {
        // send back file received message
        handler.sendMessage(new ReceivedMessage(message.header.cfId, message.header.sequenceNumber));
        receivers.get(message.header.cfId).received(message.sstable);   //这里message里的sstable是SSTableWriter.  
    }
```

receivers相关的StreamReceiveTask是在prepareReceiving创建并加入的(通过StreamSummary,即PREPARE附带的Summary信息).  
接下来的流程交给了StreamReceiveTask.received方法, 而ReceivedMessage的处理是StreamTransferTask.complete发送方的工作接近完成了.  
接收sstable文件的方式是用SSTableWriter关闭并打开SSTableReader, 加入到ColumnFamilyStore中,可能的话创建二级索引.  
```java
    public void run() {
        Pair<String, String> kscf = Schema.instance.getCF(task.cfId);
        ColumnFamilyStore cfs = Keyspace.open(kscf.left).getColumnFamilyStore(kscf.right);

        List<SSTableReader> readers = new ArrayList<>();
        for (SSTableWriter writer : task.sstables)
            readers.add(writer.closeAndOpenReader());
        // add sstables and build secondary indexes
        cfs.addSSTables(readers);
        cfs.indexManager.maybeBuildSecondaryIndexes(readers, cfs.indexManager.allIndexesNames());

        task.session.taskCompleted(task);
    }
```




















