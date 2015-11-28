---
layout: post
title: Apache Drill源码分析之execute
category: Source
tags: BigData
keywords: 
description: 
---

## 设置根节点Fragment

在前面根据物理计划生成的QueryWorkUnit, 会用于设置根节点和非根节点.  
对于根节点, 因为现在是在Foreman里, 所以是运行在Foreman节点, 相对来说就是运行在本地了.  
因为Root Fragment都是在Foreman节点执行的. 

> RootFragment不是Major Fragment, 按照官方的文档Major Fragment并不会执行真正的任务.   
>那么Minor Fragment和Leaf Fragment又有什么区别呢?  

```
  // Set up the root fragment (which will run locally), and submit it for execution.
  private void setupRootFragment(final PlanFragment rootFragment, final FragmentRoot rootOperator) throws ExecutionSetupException {
    final FragmentContext rootContext = new FragmentContext(drillbitContext, rootFragment, queryContext, initiatingClient, drillbitContext.getFunctionImplementationRegistry());
    //首先准备好IncomingBuffer
    final IncomingBuffers buffers = new IncomingBuffers(rootFragment, rootContext);
    rootContext.setBuffers(buffers);

    //每个Fragment都要加入到QueryManager的监控中,当Fragment的状态发生更新,QM可以知道
    queryManager.addFragmentStatusTracker(rootFragment, true);

    //Fragment的Executor,是真正运行Fragment的线程
    rootRunner = new FragmentExecutor(rootContext, rootFragment, queryManager.newRootStatusHandler(rootContext, drillbitContext), rootOperator);
    //Fragment的Manager,除了线程,还有Buffer
    final RootFragmentManager fragmentManager = new RootFragmentManager(rootFragment.getHandle(), buffers, rootRunner);

    if (buffers.isDone()) {
      // if we don't have to wait for any incoming data, start the fragment runner. 不需要等待接收数据,启动线程
      bee.addFragmentRunner(fragmentManager.getRunnable());
    } else {
      // if we do, record the fragment manager in the workBus. 如果需要等待,则把管理Executor的Manager放到WorkBus中
      // TODO aren't we managing our own work? What does this do? It looks like this will never get run
      drillbitContext.getWorkBus().addFragmentManager(fragmentManager);
    }
  }
```

在FragmentExecutor线程的run方法里会创建RootExec

```
          root = ImplCreator.getExec(fragmentContext, rootOperator);
          //Run the query until root.next returns false OR we no longer need to continue.
          while (shouldContinue() && root.next()) {
            // loop
          }
          return null;
```

这里就要涉及到Drill底层读取数据的数据结构了.  对了,前面的IncomingBuffers到底是用来干嘛的?  



