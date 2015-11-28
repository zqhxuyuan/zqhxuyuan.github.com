---
layout: post
---

## UbuntuServer

物理机环境： UbuntuDesktop32Bits  
虚拟机环境： UbuntuServer-64Bits

### 1. Install UbuntuServer-64Bits 

1. virtualbox下载一个UbuntuServer.然后在虚拟机的UbuntuServer上安装docker. 
2. 使用vagrant和virtualbox: vagrant下载64位的UbuntuServer的box
   vagrant的box实际上也是一个虚拟机. 然后可以在这里安装docker. 

> 注意: 要下载内核版本>3.8的UbuntuServer, 比如12.04.4, 不能下载10.4,  
参考https://github.com/astaxie/Go-in-Action/blob/master/ebook/zh/01.2.md 安装配置vagrant. 
下载box的地址是: http://www.vagrantbox.es/ 

```
mkdir ~/vagrant
cd vagrant   
vagrant box add base xxx.box    将下载的xxx.box放到~/vagrant中,添加box:
vagrant init   初始化
vagrant up    启动, 启动后可以在virtualbox中看到自动添加了一个虚拟机, 名称: vagrant_default_xxxxx
vagrant ssh   连接到虚拟机, 这样我们就能进入下载好的box的虚拟环境中.  
```

### 2. Install Docker

· 必须确保Linux的内核版本>3.8. 如果不是执行以下命令升级内核: 

```
zqhxuyuan@zqh:~$ ssh zqh@UBS
zqh@ubuntu:~$ sudo apt-get update 
zqh@ubuntu:~$ sudo apt-get install linux-image-generic-lts-raring linux-headers-generic-lts-raring 
zqh@ubuntu:~$ sudo reboot  或者sudo shutdown -r now重启.(-h关机) 
```

· 然后安装docker:

```
zqh@ubuntu:~$ sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 36A1D7869245C8950F966E92D8576A8BA88D21E9 
zqh@ubuntu:~$ sudo sh -c "echo deb http://get.docker.io/ubuntu docker main /etc/apt/sources.list.d/docker.list" 
zqh@ubuntu:~$ sudo apt-get update 
zqh@ubuntu:~$ sudo apt-get install lxc-docker
```

· 查看docker版本: 

```
zqh@ubuntu:~$ sudo docker version
Client version: 0.9.0
Go version (client): go1.2.1
Git commit (client): 2b3fdf2
Server version: 0.9.0
Git commit (server): 2b3fdf2
Go version (server): go1.2.1
Last stable version: 0.9.0
```

### 3. Run A Container

·下载ubuntu镜像并执行该容器:

```
zqh@ubuntu:~$ sudo docker run -i -t ubuntu /bin/bash
root@cb5ddcdda55c:/# exitzqh@ubuntu:~$ 
```

在UbuntuServer中下载docker就好比docker是一个普通的软件.  
通过**```docker run -i -t ubuntu /bin/bash```**这个时候是把docker当做一个类似于virtualbox的软件  
因为我们知道可以在virtualbox中新建多个虚拟机. 而docker也正是以类似的方式,可以运行多个虚拟机的镜像.  
要理解这层关系,你可以把运行docker的机器就当做主机来看. 而通过docker run ubuntu /bin/bash进入的是虚拟机.

|主机|虚拟机|
|----|----|
|docker|Ubuntu, CentOS, ....|

要从虚拟机(run进去的镜像)退出到主机(docker所在的主机)只要在虚拟机中敲入: exit  

![](http://7xjs7x.com1.z0.glb.clouddn.com/docker1.png)

类似的在docker index中有很多已经制作好的镜像(和上面的docker run ubuntu类似), 比如:  

|docker command|tips|
|--------------|----|
|docker pull learn/tutorial|下载别人已经制作好的镜像|
|docker run learn/tutorial /bin/echo hello world|
|docker run -i -t learn/tutorial /bin/bash<br>root@51774a81beb3:/#|以bash命令进入到镜像中<br>现在我们已经进入到一个虚拟的linux镜像中了

### 4. container commit & pull

> 注意: 通过sudo docker run -i -t XXX /bin/bash进入的XXX镜像敲入的任何命令, 在exit之后  
> 如果重新sudo docker run -i -t XXX /bin/bash, 则之前敲入的任何命令都无效. 要想起作用必须commit:

下面的步骤我们执行了两次```sudo docker run -i -t zq xu yuan/tutorial /bin/bash```  
其中第一次run在home下创建了zqh目录,exit后,如果再次执行run命令,则是看不到home下的zqh.  
只有把第一次的步骤提交之后,然后执行run才可以看到zqh.  

```
zqh@ubuntu:~$ sudo docker run -i -t zqhxuyuan/tutorial /bin/bash
root@4295ee9b0501:/# ls
root@4295ee9b0501:/# cd home
root@4295ee9b0501:/home# ls
root@4295ee9b0501:/home# mkdir zqh
root@4295ee9b0501:/home# exit
exit
zqh@ubuntu:~$ sudo docker run -i -t zqhxuyuan/tutorial /bin/bash
root@b9a2dfca03fc:/# ls /home    				-> 没有提交第一次run,看不到zqh
root@b9a2dfca03fc:/# exit
exit
zqh@ubuntu:~$ sudo docker ps -a 				-> 按照时间顺序,靠近当前的排在前面,所以要找到第一次的那个时间点
CONTAINER ID IMAGE COMMAND CREATED STATUS PORTS NAMES
b9a2dfca03fc zqhxuyuan/tutorial:latest /bin/bash 27 seconds ago Exit 0 determined_heisenberg  	-> 第二次run
4295ee9b0501 zqhxuyuan/tutorial:latest /bin/bash 57 seconds ago Exit 0 thirsty_engelbart     	-> 第一次run
zqh@ubuntu:~$ sudo docker commit 4295ee9b0501 zqhxuyuan/tutorial  	-> 将第一次run的容器ID提交到自己的index下.
4f2b1958e07871718a9aa2734d235ed2260e1ea7a7604ecd30cbf6c25ce7d3e0
zqh@ubuntu:~$ sudo docker run -i -t zqhxuyuan/tutorial /bin/bash
root@14aefe2b2d86:/# ls home    			-> 因为已经提交了,接下来的run都可以看到home下的zqh
zqh
root@14aefe2b2d86:/# exit
exit
zqh@ubuntu:~$ sudo docker pull zqhxuyuan/tutorial     	-> commit是放在本地,可以pull到docker index上
Pulling repository zqhxuyuan/tutorial 			-> 要下载到本地, 因为已经在本地了, 所以实际上不需要
30f4005e2085: Download complete 
8dbd9e392a96: Download complete 
cc2d5d37dc39: Download complete 
zqh@ubuntu:~$
```

然后可以打开 https://index.docker.io/account/# 	登陆你自己的账号,可以看到respositories的Pulls次数+1

![](http://7xjs7x.com1.z0.glb.clouddn.com/docker2.png)

记住主要的四个命令:  

```
1. sudo docker pull CONTAINER
2. sudo docker run -i -t CONTAINER /bin/bash
3. 通过sudo docker ps -a 找到上次操作的时间点,提交到本地库中
sudo docker commit CONTAINERID CONTAINER
4. sudo docker pull CONTAINER
```

至于这些命令的执行在主机上的任何地方都可以. 也就是说sudo docker后面跟的命令相当于自己建立了一个库(类似git库).  
通常可以从docker index中pull一个别人创建好的container, 然后commit时使用自己的账号, 并pull到docker index中.  

通过```sudo docker images```可以查看本机已经下载过的所有镜像. 包括往docker index push的镜像.  

```
zqh@ubuntu:~$ sudo docker images
REPOSITORY TAG IMAGE ID CREATED VIRTUAL SIZE
zqhxuyuan/tutorial latest 30f4005e2085 About an hour ago 180.8 MB
zqhxuyuan/centos-node-hello latest 094dbb964e34 3 hours ago 664.8 MB
<none> <none> e8e000f5673a 3 hours ago 664.8 MB
<none> <none> 261de7bc3171 14 hours ago 664.8 MB -> You Know GFW, can't pull to docker index! 上面截图的Initialized就是这个原因
zqhxuyuan/ping latest 9a3d34a8954c 17 hours ago 204.4 MB
learn/ping latest a90b3449497c 18 hours ago 139.5 MB
ubuntu 13.10 9f676bd305a4 5 weeks ago 178 MB
ubuntu precise 9cd978db300e 5 weeks ago 204.4 MB
busybox latest 769b9341d937 5 weeks ago 2.489 MB
learn/tutorial latest 8dbd9e392a96 11 months ago 128 MB
centos 6.4 539c0211cd76 11 months ago 300.6 MB
```

### 5. DockerFile & build + install sshd

从前面知道镜像就是类似于虚拟机, 我们可以在镜像上安装任何软件. 从前面的主机UbuntuDesktop通过ssh访问虚拟机UbuntuServer.  
而在UbuntuServer中安装docker, docker中可以运行镜像(docker run), 镜像也类似虚拟机, 因此如果我们把docker所在的机器当做主机,  
那么docker运行的镜像就是虚拟机了. 我们可以不可以不用通过docker run XXX /bin/bash进入虚拟机, 而是通过ssh直接连接镜像?  
这样我们就能从主机Host经过两次ssh连接到docker创建出来的镜像中, 如下图所示:  

![](http://7xjs7x.com1.z0.glb.clouddn.com/docker3.png)

只要给镜像安装sshd模块, 这样docker所在的机器就可以通过ssh访问镜像了.  
还是使用前面的zqhxuyuan/tutorial, 在这个镜像上安装sshd服务.  
只有在镜像上安装sshd, 才可以在docker所在的机器通过ssh连接到镜像上.  

1. 创建Dockerfile文件

```
zqh@ubuntu:~/dev/docker-sshd$ cat Dockerfile 
FROM zqhxuyuan/tutorial			-> 等价于sudo docker pull zqhxuyuan/tutorial
MAINTAINER z qh
RUN apt-get update
RUN apt-get install -y openssh-server
RUN mkdir /var/run/sshd
RUN echo "root:root" | chpasswd
EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]
```

2. 构建镜像

在Dockerfile所在的目录构建镜像, 注意container后面的点表示当前目录

```
zqh@ubuntu:~/dev/docker-sshd$ sudo docker build -t zqhxuyuan/tutorial .
Uploading context  2.56 kB
Uploading context 
Step 0 : FROM zqhxuyuan/tutorial  ---> f9493de1cea7
Step 1 : MAINTAINER z qh   		---> Running in 2dcc96bbc6a3  ---> 3cd796488fe0
Step 2 : RUN apt-get update  	---> Running in 332cac8820f5
Ign http://archive.ubuntu.com precise InRelease
Hit http://archive.ubuntu.com precise Release.gpg
Hit http://archive.ubuntu.com precise Release
Hit http://archive.ubuntu.com precise/main amd64 Packages
Hit http://archive.ubuntu.com precise/main i386 Packages
Hit http://archive.ubuntu.com precise/main Translation-en	---> 47fe9c2aeb97
Step 3 : RUN apt-get install -y openssh-server   		---> Running in 2ca878275fc0
openssh-server is already the newest version.
0 upgraded, 0 newly installed, 0 to remove and 0 not upgraded.   ---> a9ba75abd3f0
Step 4 : RUN mkdir /var/run/sshd  				---> Running in 919fc8dd20b9
2014/03/16 17:01:58 The command [/bin/sh -c mkdir /var/run/sshd] returned a non-zero code: 1
```

通常我们可以在Dockerfile中的FROM获取docker index中已有的镜像, 然后build -t的名称为我们自己的镜像.  
比如FROM ubuntu,    build -t zqhxuyuan/ubuntu

3. 启动sshd服务

```
zqh@ubuntu:~/dev/docker-sshd$ sudo docker run -p 2222:22 -d zqhxuyuan/tutorial /usr/sbin/sshd -D
c21628712c88bdb6c0ccfbe8512ac76c19e05068ce47a3bdb2a00bb911ec5646
```

这样我们就以后台进程启动了sshd服务了. 最后的参数-D表示daemon进程, -p表示端口绑定  
对于docker是第一个参数2222, 对于container暴露出来的端口是22.  

4. 查看docker进程

```
zqh@ubuntu:~/dev/docker-sshd$ sudo docker ps
CONTAINER ID  IMAGE             COMMAND        CREATED       STATUS        PORTS               NAMES
c21628712c88 zqhxuyuan/tutorial:latest /usr/sbin/sshd -D   5 minutes ago    Up 5 minutes    0.0.0.0:2222->22/tcp   romantic_lovelace  
```

5. 在docker中通过ssh连接镜像. 而不需要通过sudo docker run -i -t zqhxuyuan/tutorial /bin/bash

```
zqh@ubuntu:~/dev/docker-sshd$ ssh root@localhost -p 2222
root@localhost's password: 		-> 这里输入密码root, 即Dockerfile中指定的密码
Welcome to Ubuntu 12.04 LTS (GNU/Linux 3.11.0-15-generic x86_64)
root@c21628712c88:~# 
```

6. 停止sshd  
前面查看docker进程时知道了container_id, 通过sudo docker kill container_id即可停止  

```
zqh@ubuntu:~/dev/docker-sshd$ sudo docker kill c21628712c88
```

## 参考文档: 
Docker官方安装手册: <http://docs.docker.io/en/latest/installation/ubuntulinux/>  
Docker:利用Linux容器实现可移植的应用部署: <http://blog.sae.sina.com.cn/archives/2896>  
Docker学习笔记-创建Java Tomat: <http://www.blogjava.net/yongboy/archive/2013/12/12/407498.html>  
Day21: Docker入门教程: <http://segmentfault.com/a/1190000000366923>  
