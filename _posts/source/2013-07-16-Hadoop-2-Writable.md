---
layout: post
title: Hadoop源码分析之Writable
category: Source
tags: BigData
keywords: 
description: 
---

###Java序列化
**序列化Serialization**:将结构化的对象转化为字节流,以便在网络上传输或写入硬盘进行永久存储;   
**反序列化deserialization**:将字节流转回到结构化对象的过程.  

```java
public class JavaSerialize { // 2-1
	public static void main(String[] args) throws Exception {
		Block block = new Block(111L, 222L, 333L);
		byte[] bytes = serialize(block);
		Block block2 = deserialize(bytes);
		System.out.println(block2);
	}
	
	private static byte[] serialize(Block block) throws IOException{
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream objOut = new ObjectOutputStream(out);
		objOut.writeObject(block);
		objOut.close();
		return out.toByteArray();
	}
	
	private static Block deserialize(byte[] bytes) throws IOException, ClassNotFoundException{
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		ObjectInputStream objIn = new ObjectInputStream(in);
		objIn.close();
		return (Block)objIn.readObject(); 
	}
}

class Block implements Serializable{
	private long blockId;
	private long genStamp;
	private long numBytes;
	public Block(){this(0L, 0L, 0L)}
	public Block(long blockId, long genStamp, long numBytes) {
		this.blockId = blockId;
		this.genStamp = genStamp;
		this.numBytes = numBytes;
	}
}
```

Java的IO采用了**装饰模式**, 上面的测试示例中ObjectInputStream封装了ByteArrayInputStream, 扩展了字节数组的功能.


###Hadoop序列化

在分布式系统中进程将对象序列化为字节流, 通过网络传输到另一进程, 另一进程接收到字节流, 通过反序列化转回到结构化对象, 以达到进程间通信.  
Hadoop系统中多个节点上进程间的通信是通过RPC实现的. RPC协议将消息(结构化对象)序列化成二进制流(字节流)后发送到远程节点, 远程节点接着将二进制流反序列化为原始消息.  


Hadoop HDFS的通信以及MapReduce的Mapper,Combiner,Reducer等阶段之间的通信都需要使用序列化与反序列化技术.   
举例来说, Mapper产生的中间结果<key: value1, value2...>需要写入到本地硬盘, 这是序列化过程(将结构化对象转化为字节流, 并写入硬盘),   
而Reducer阶段读取Mapper的中间结果的过程则是一个反序列化过程(读取硬盘上存储的字节流文件, 并转回为结构化对象).   
需要注意的是, 能够在网络上传输的只能是字节流, Mapper的中间结果在不同主机间洗牌shuffle时, 对象将经历序列化和反序列化两个过程.  

![Hadoop 序列化](https://bbd7kw.blu.livefilestore.com/y2pv18wN8Vvt4o-Isvx_B3fIRqAhQhAChOOlE7WhJTS8NrJh5eOGfhniaJDt19fQig0B67AECKRxtU-2ubddYnt3gJ41yednRm0scry-2a_aaftD4hZ_NL7gTasP5eNDRJJ/1-Writable.png?psid=1)

```java
/** A serializable object which implements a simple, efficient, serialization protocol, based on DataInput and DataOutput.
 * Any key or value type in the Hadoop Map-Reduce framework implements this interface. 
 * Implementations typically implement a static read(DataInput) method which constructs a new instance,  
 * calls #readFields(DataInput) and returns the instance. */
public interface Writable {
  /** Serialize the fields of this object to out.
   * @param out DataOuput to serialize this object into. */
  void write(DataOutput out) throws IOException;

  /** Deserialize the fields of this object from in.  为了效率,尽可能复用现有的对象 
   * For efficiency, implementations should attempt to re-use storage in the existing object where possible.
   * @param in DataInput to deseriablize this object from. */
  void readFields(DataInput in) throws IOException;
}
```

实现了Writable接口的一个典型例子如下:

```java
public class MyWritable implements Writable {
	private int counter; // Some data
	private long timestamp;

	public void write(DataOutput out) throws IOException {
		out.writeInt(counter);
		out.writeLong(timestamp);
	}
	public void readFields(DataInput in) throws IOException {
		counter = in.readInt();
		timestamp = in.readLong();
	}
	public static MyWritable read(DataInput in) throws IOException {
		MyWritable w = new MyWritable();
		w.readFields(in);
		return w;
	}
}
```


下面的代码来自于Hadoop权威指南的测试示例(经过简单加工) 测试用例2-2

```java
public class WritableTest extends TestCase{
	@Test // 2-2
	public void testIntWritable() throws IOException{
		IntWritable writable = new IntWritable(163);
		byte[] bytes = serialize(writable);

		System.out.println(bytes.length == 4); // true, 一个int整数占用4个字节
		System.out.println(StringUtils.byteToHexString(bytes).equals("000000a3")); // 4个字节,8位(16进制)

		IntWritable newWritable = new IntWritable();
		deserialize(newWritable, bytes); // 从我们刚写的输出数据(bytes)中读取数据(Writable对象)
		System.out.println(newWritable.get() == 163); // true 
	}
	
	// Writable序列化成byte[]: Writable --> byte[]
	public static byte[] serialize(Writable writable) throws IOException{
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		writable.write(dataOut);
		dataOut.close();
		return out.toByteArray();
	}
	
	// byte[]反序列化成Writable: byte[] --> Writable
	public static byte[] deserialize(Writable writable, byte[] bytes) throws IOException{
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dataIn = new DataInputStream(in);
		writable.readFields(dataIn);
		dataIn.close();
		return bytes;
	}
}
```

上面的测试示例中DataInputStream实现了DataInput接口, 并封装了ByteArrayInputStream扩展了字节数组的功能.  

###WritableComparable
对MapReduce来说, 类型的比较是非常重要的, 因为中间有个基于键的排序阶段

```java
/** A Writable which is also Comparable. 
 * WritableComparables can be compared to each other, typically via Comparators. 
 * Any type which is to be used as a key in the Hadoop Map-Reduce framework should implement this interface. */
public interface WritableComparable<T> extends Writable, Comparable<T> {
}
```

####IntWritable
实现WritableComparable的类有IntWritable, BooleanWritable等

```java
/** A WritableComparable for ints. */
public class IntWritable implements WritableComparable {
  private int value;
  // 省略构造函数, set, get, equals, hashCode方法
  public void readFields(DataInput in) throws IOException {
    value = in.readInt();
  }
  public void write(DataOutput out) throws IOException {
    out.writeInt(value);
  }
  public int compareTo(Object o) { /** Compares two IntWritables. */
    int thisValue = this.value;
    int thatValue = ((IntWritable)o).value;
    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
  }
}
```

###WritableComparator
IntWritable实现了WritableComparable类, 并通过compareTo来进行对象的比较. Hadoop还提供了一个更高效的比较接口RawComparator.  
关于Comparable和Comparator的区别,  [Java Sorting: Comparator vs Comparable Tutorial](http://www.digizol.com/2008/07/java-sorting-comparator-vs-comparable.html)这篇文章给了解释  
一般而言我们会让对象实现Comparable接口, 进行该对象和传入的对象的比较, 而新建一个Comparator比较器来比较两个不同的对象  

static块在类加载的时候运行进行注册: 

```java
  static {   // register this comparator 注册比较器, IntWritable定义了自己的内部类比较器
    WritableComparator.define(IntWritable.class, new Comparator());
  }
```

WritableComparator静态方法define用于注册实现WritableComparable(比如IntWritable)的内部类比较器.  
实际是放入一个Map中, key为实现WritableComparable(IntWritable)的Class类型, value为该比较器.  

```java
/** A Comparator for WritableComparables.
 * This base implemenation uses the natural ordering.  To define alternate orderings, override #compare(WritableComparable,WritableComparable).
 * 
 * One may optimize compare-intensive operations by overriding #compare(byte[],int,int,byte[],int,int).
 * Static utility methods are provided to assist in optimized implementations of this method. */
public class WritableComparator implements RawComparator {
  private static HashMap<Class, WritableComparator> comparators = new HashMap<Class, WritableComparator>(); // registry

  /** Get a comparator for a WritableComparable implementation. */
  public static synchronized WritableComparator get(Class<? extends WritableComparable> c) {
    WritableComparator comparator = comparators.get(c);
    if (comparator == null) comparator = new WritableComparator(c, true);
    return comparator;
  }

  /** Register an optimized comparator for a WritableComparable implementation. */
  public static synchronized void define(Class c, WritableComparator comparator) {
    comparators.put(c, comparator);
  }
}
```

RawComparator继承Java Comparator,允许其实现直接比较字节流中的记录, 无需先把字节流反序列化为对象再进行比较,避免了新建对象的额外开销.

```java
/** A Comparator that operates directly on byte representations of objects. */
public interface RawComparator<T> extends Comparator<T> {
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2);
}
```

以IntWritable的内部类Comparator为例, 方法compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)就是对RawComparator的compare方法的实现.  
该方法可以从每个字节数组byte[] b1和b2中读取给定起始位置(s1,s2)以及长度(l1,l2)的一个整数, 通过readInt()直接在字节数组中读入需要比较的两个整数, 该过程并没有使用IntWritable对象, 从而避免了不必要的对象分配. 后面有针对此的一个测试用例2-3.  
所以说IntWritable.Comparator是一个优化的比较方法. 实际上WritableComparator也默认实现了compare方法, 但IntWritable.Comparator进行了重写.  

```java
  // IntWritable的内部类Comparator比较器 >> WritableComparator --|> RawComparator接口 >> Comparator
  /** A Comparator optimized for IntWritable. */ 
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(IntWritable.class); // 构造函数传递IntWritable的Class类型
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int thisValue = readInt(b1, s1);
      int thatValue = readInt(b2, s2);
      return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }
  }
```

现在来看IntWritable.Comparator的构造函数通过super调用到父类WritableComparator的构造器

```java
public class WritableComparator implements RawComparator {
  private final Class<? extends WritableComparable> keyClass;
  private final WritableComparable key1;
  private final WritableComparable key2;
  private final DataInputBuffer buffer;

  /** Construct for a WritableComparable implementation. */
  protected WritableComparator(Class<? extends WritableComparable> keyClass) {
    this(keyClass, false);
  }
  protected WritableComparator(Class<? extends WritableComparable> keyClass, boolean createInstances) {
    this.keyClass = keyClass;
    key1 = key2 = null;
    buffer = null;
  }

  /** Optimization hook.  Override this to make SequenceFile.Sorter's scream.
   * The default implementation reads the data into two WritableComparables  
   * (using Writable#readFields(DataInput), then calls #compare(WritableComparable,WritableComparable). */
  public int compare(byte[] b1, int s1,int l1, byte[] b2, int s2,int l2) { // RawComparator的比较方法, WritableComparator实现类重写该方法则以重写的方法比较
    buffer.reset(b1, s1, l1);   	// parse key1
    key1.readFields(buffer);  		// read buffer data into WritableComparable
    buffer.reset(b2, s2, l2);   	// parse key2
    key2.readFields(buffer);
    return compare(key1, key2); 	// compare them
  }

  /** Compare two WritableComparables. The default implementation uses the natural ordering, calling Comparable#compareTo(Object). */
  public int compare(WritableComparable a, WritableComparable b) {
    return a.compareTo(b);		// Comparable的比较方法, WritableComparable实现类实现该方法则以实现的方法比较
  }

  public int compare(Object a, Object b) { 	// RawComparator的父类Comparator的比较方法
    return compare((WritableComparable)a, (WritableComparable)b);
  }
}
```

WritableComparator定义了多个同名的compare比较方法:   
实现RawComparator的比较方法compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)  
以及父类RawComparator继承的Comparator比较方法compare(Object a, Object b)  
这两个方法都调用compare(WritableComparable a, WritableComparable b)比较两个WritableComparable对象.  
该方法调用了Comparable的compareTo方法. 但由于Comparable是个接口, 所以会调用WritableComparable实现类的方法.  

同样以WritableComparable的实现类IntWritable为例, IntWritable实现了Comparable的compareTo(Object o)方法.  
所以WritableComparator的compare(WritableComparable a, WritableComparable b)调用的a.compareTo(b)最终调用了IntWritable.compareTo(Object o)方法.  

###Comparable+Comparator测试用例

我们来用Eclipse的Debug工具来证明compare(WritableComparable a, WritableComparable b)最终调用到IntWritable.compareTo(Object o)方法  

```java
@Test // 2-3
public void testIntWritableComparable() throws IOException{
    IntWritable w1 = neIntWritablew (162);
		IntWritable w2 = new IntWritable(163);
		
		RawComparator<IntWritable> comparator = WritableComparator.get(IntWritable.class);  // ①
		System.out.println(comparator.compare(w1, w2) < 0);	// ②  ⑨
}

public class WritableComparator implements RawComparator {
  public int compare(WritableComparable a, WritableComparable b) { // ③
    return a.compareTo(b); // ④  ⑦
  }

  public int compare(Object a, Object b) { // ②
    return compare((WritableComparable)a, (WritableComparable)b); // ③  ⑧
  }
}

public class IntWritable implements WritableComparable {
  public int compareTo(Object o) { // ⑤
    int thisValue = this.value;
    int thatValue = ((IntWritable)o).value;
    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1)); // ⑥
  }
}
```

① 为了获得WritableComparator对象, 我们使用WritableComparator.get()方法根据类型来获取一个比较器:  
前面说过通过static块进行类型和比较器<IntWritable.class, IntWritable.Comparator>的注册, 有put就有get.  
WritableComparator.get(Class)方法用于根据Class的类型(IntWritable)获取定义在该类型中的比较器. 
比如为了获得IntWritable的Comparator, 如下调用
    
    RawComparator<IntWritable> comparator = WritableComparator.get(IntWritable.class); // 注意这里返回的是接口类型 
	
实际上通过此方法返回的是IntWritable的比较器即IntWritable.Comparator内部类.  
IntWritable.Comparator还实现了RawComparator的字节流比较方法, 后面会据此进行另一个测试用例. 
  
② 调用RawComparator(Comparator)的compare(Object o1, Object o2)方法比较两个IntWritable对象,  
因为RawComparator的实现类WritableComparator实现了compare(Object a, Object b),  
而IntWritable.Comparator没有实现该方法, 所以会调用到WritableComparator.compare(Object a, Object b)  

③ 因为比较的是IntWritable类型, 所以可以转为WritableComparable, 但是这里的参数a, b仍然是IntWritable类型的.  
④ 由于IntWritable实现了compareTo比较方法, 所以WritableComparator中的compareTo方法最终调用的是IntWritable实现的compareTo方法  
⑤ 进行对象的比较.  

上面说过IntWritable.Comparator实现了RawComparator的优化的字节流比较方法, 对应的测试用例:

```java
	@Test // 2-4
	public void testIntWritableComparator() throws IOException{		
		IntWritable w1 = new IntWritable(162);
		IntWritable w2 = new IntWritable(163);
		byte[] b1 = serialize(w1);
		byte[] b2 = serialize(w2);
		RawComparator<IntWritable> comparator = WritableComparator.get(IntWritable.class);
		System.out.println(comparator.compare(b1, 0, b1.length, b2, 0, b2.length) < 0); // ①
	}

public class IntWritable implements WritableComparable {
  public static class Comparator extends WritableComparator {
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) { // ②
      int thisValue = readInt(b1, s1);
      int thatValue = readInt(b2, s2);
      return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }
  }
}

public class WritableComparator implements RawComparator {
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) { // RawComparator的比较方法
    buffer.reset(b1, s1, l1);   	// parse key1
    key1.readFields(buffer);  		// read buffer data into WritableComparable
    buffer.reset(b2, s2, l2);   	// parse key2
    key2.readFields(buffer);
    return compare(key1, key2); 	// compare them
  }
}
```

① 上面说过WritableComparator.get(IntWritable.class)返回的实际是IntWritable.Comparator. 
调用字节流比较方法compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) 
虽然WritableComparator实现了该方法, 但是由于IntWritable.Comparator也重写了该方法, 所以最后调用的是IntWritable.Comparator的方法.
采用Debug调试时, 分别在IntWritable.Comparator和WritableComparator的该方法上设置断点, 进入的只是IntWritable.Comparator的方法.

总结: Comparable比较是在对象上比较, Comparator是直接在字节流上比较, 这种方式是一种高效的优化的比较方式.

> WritableComparator is a general-purpose implementation of RawComparator for WritableComparable classes. 
It provides two main functions. First, it provides a default implementation of the raw compare() method that deserializes the objects to be compared from the stream and invokes the object compare() method. Second, it acts as a factory for RawComparator instances (that Writable implementations have registered).
WritableComparator是RawComparator对WritableComparable类的一个通用实现. 提供了两个功能: 一:提供了对原始compare()的一个默认实现, 该方法能够反序列化将在流中进行比较的对象, 并调用对象的compare(); 二: 充当了RawComparator实例的工厂方法,可以通过get(IntWritable.class)获得IntWritable的Comparator.


下面是目前介绍的各个类的结构图(StarUML):  
![Hadoop Writable](https://n4tfqg.blu.livefilestore.com/y2pB4zE2NxhTFpmS-DAcupZ1oI_c9kxXqDloPQh--xH8wwhrARt8klClbTx1BEjSq_2LmFbWGiq3H2xy9JpZUvyIK6wIAvdhmo-3ZTKYowpz7S57tn1fHKYHpO_fb6P3A4n/2-2%20Writable.png?psid=1)

