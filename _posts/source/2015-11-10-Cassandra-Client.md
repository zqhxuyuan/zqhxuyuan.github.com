---
layout: post
title: CassandraClient查询优化
category: Source
tags: BigData
keywords: 
description: 
---

CassandraClient查询: session.execute()

```java
public ResultSet execute(Statement statement) {
    return executeAsync(statement).getUninterruptibly();
}
```

注: Statement包括了insert和select. 

### 异步+不能中断

executeAsync是异步执行的(AbstractSession):  

```java
public ResultSetFuture executeAsync(Statement statement) {
    return executeQuery(makeRequestMessage(statement, null), statement);
}
```

但是却是Uninterrupted不能中断的: 等待结果返回是不能中断的(DefaultResultSetFuture).   

```java
    /**
     * Waits for the query to return and return its result.
     *
     * This method is usually more convenient than {@link #get} because it:
     * <ul>
     *   <li>Waits for the result uninterruptibly, and so doesn't throw InterruptedException}.
     *   <li>Returns meaningful exceptions, instead of having to deal with ExecutionException.</li>
     * </ul>
     * As such, it is the preferred way to get the future result.  
     *
     * @throws NoHostAvailableException if no host in the cluster can be contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, that is an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     */
    public ResultSet getUninterruptibly() {
        try {
            return Uninterruptibles.getUninterruptibly(this);
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }    
```

Uninterrupted的实现:  

```java
  public static <V> V getUninterruptibly(Future<V> future) throws ExecutionException {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          return future.get();
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
```
这种对于Future而言是适合的, 而Future的特点是要等到执行完成才有返回值. 如果Future执行时间很长, 就会导致查询Cassandra超时.  

>疑问: Cassandra中设置的read time out=2s为什么没有起作用?  可能是driver客户端本身存在的问题. 

结论: 执行statement虽然使用executeAsync异步查询, 但是获取返回值是不能中断的: 包装上Future直到返回结果. 

### 带超时的Future

DefaultResultSetFuture还提供了一个带超时的Future:  

```java
    //Waits for the provided time for the query to return and return its result if available.
    public ResultSet getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            return Uninterruptibles.getUninterruptibly(this, timeout, unit);
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }
```

超时的控制通过将时间传入future, 如果到时间了还没有返回值, 则中断:  

```java
  public static <V> V getUninterruptibly(Future<V> future, long timeout,  TimeUnit unit) throws ExecutionException, TimeoutException {
    boolean interrupted = false;
    try {
      long remainingNanos = unit.toNanos(timeout);
      long end = System.nanoTime() + remainingNanos;

      while (true) {
        try {
          // Future treats negative timeouts just like zero.
          return future.get(remainingNanos, NANOSECONDS);
        } catch (InterruptedException e) {
          interrupted = true;
          remainingNanos = end - System.nanoTime();
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
```

这种调用应该可以解决无法中断的问题.  但是AbstractSession的方法中没有提供带超时时间的execute方法.  
一种方式是修改AbstractSession添加对应的方法. 并在CassandraClient中使用这种带超时的方式.  
但这种方式需要修改源码, 并打包.  

### 直接在CassandraClient控制  

DefaultResultSetFuture的cancel上说明了怎么使用Timeout:  
但是这种方式只是取消客户端查询,并不会取消在Cassandra服务端已经开始的请求.  

```java
    /**
     * Attempts to cancel the execution of the request corresponding to this
     * future. This attempt will fail if the request has already returned.
     * <p>
     * Please note that this only cancels the request driver side, but nothing
     * is done to interrupt the execution of the request Cassandra side (and that even
     * if {@code mayInterruptIfRunning} is true) since  Cassandra does not
     * support such interruption.
     * <p>
     * This method can be used to ensure no more work is performed driver side
     * (which, while it doesn't include stopping a request already submitted
     * to a Cassandra node, may include not retrying another Cassandra host on
     * failure/timeout) if the ResultSet is not going to be retried. Typically,
     * the code to wait for a request result for a maximum of 1 second could
     * look like:
     * <pre>
     *   ResultSetFuture future = session.executeAsync(...some query...);
     *   try {
     *       ResultSet result = future.get(1, TimeUnit.SECONDS);
     *       ... process result ...
     *   } catch (TimeoutException e) {
     *       future.cancel(true); // Ensure any resource used by this query driver
     *                            // side is released immediately
     *       ... handle timeout ...
     *   }
     * <pre>
     *
     * @param mayInterruptIfRunning the value of this parameter is currently
     * ignored.
     * @return {@code false} if the future could not be cancelled (it has already
     * completed normally); {@code true} otherwise.
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (!super.cancel(mayInterruptIfRunning))
            return false;

        handler.cancel();
        return true;
    }
```

所以CassandraClient的查询可以改造成:  

```java
    //带超时的SQL
    public ResultSet getResultSetTimeout(BoundStatement bstmt) {
        ResultSetFuture future = session.executeAsync(bstmt);
        try {
            ResultSet result = future.get(500, TimeUnit.MILLISECONDS);
            return result;
        } catch (TimeoutException e) {
            future.cancel(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }
    
    public List<Row> getRowTimeout(BoundStatement bstmt){
        ResultSet resultSet = getResultSetTimeout(bstmt);
        if(null == resultSet) return null;

        List<Row> resultRows = new ArrayList<>();
        Iterator<Row> it = resultSet.iterator();
        while (it.hasNext()) {
            resultRows.add(it.next());
        }
        return resultRows;
    }
```
