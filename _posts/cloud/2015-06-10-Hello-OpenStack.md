---
layout: post
---

ub14.04下devstack安装openstack:	<http://f.dataguru.cn/thread-371472-1-1.html>  
Ubuntu14.04下安装DevStack: 		<http://www.cnblogs.com/JimMoriarty/p/3798724.html>  
devstack安装和测试:					<http://www.chenshake.com/devstack-installation-and-testing/>  
devstack安装使用openstack常见问题与解决办法: 	<http://blog.csdn.net/halcyonbaby/article/details/25829651>   

通过devstack自动部署Openstack icehouse版本:	<http://lj119.blog.51cto.com/605062/1427870>  
CentOS下一键安装OpenStack:					<http://www.linuxidc.com/Linux/2014-01/94926.htm>  
PackStack 自动化部署 之RDO Openstack:			<http://lj119.blog.51cto.com/605062/1427960>  

```
hadoop@hadoop:~$ cd ~/github/openstack
git clone https://github.com/openstack/nova.git
git clone https://github.com/openstack/glance.git
git clone https://github.com/openstack/cinder.git
git clone https://github.com/openstack/horizon.git
git clone https://github.com/openstack/keystone.git
git clone https://github.com/openstack/neutron.git
git clone https://github.com/openstack/swift.git
git clone https://github.com/openstack/heat.git
git clone https://github.com/openstack/ceilometer.git
git clone https://github.com/openstack/neutron-fwaas.git
git clone https://github.com/openstack/neutron-lbaas.git
git clone https://github.com/openstack/neutron-vpnaas.git
git clone https://github.com/openstack/heat-cfntools.git
git clone https://github.com/openstack/trove.git

➜  ~  ll github/openstack
总用量 68K
drwxrwxr-x  9 hadoop hadoop 4.0K Dec 25  2014 ceilometer
drwxrwxr-x  8 hadoop hadoop 4.0K Dec 25  2014 cinder
drwxrwxr-x 12 hadoop hadoop 4.0K Dec 24  2014 devstack
drwxrwxr-x  8 hadoop hadoop 4.0K Dec 25  2014 glance
drwxrwxr-x 11 hadoop hadoop 4.0K Dec 25  2014 heat
drwxrwxr-x  7 hadoop hadoop 4.0K Dec 25  2014 heat-cfntools
drwxrwxr-x  8 hadoop hadoop 4.0K Dec 25  2014 horizon
drwxrwxr-x 11 hadoop hadoop 4.0K Dec 25  2014 keystone
drwxrwxr-x  9 hadoop hadoop 4.0K Dec 25  2014 neutron
drwxrwxr-x  7 hadoop hadoop 4.0K Dec 25  2014 neutron-fwaas
drwxrwxr-x  7 hadoop hadoop 4.0K Dec 25  2014 neutron-lbaas
drwxrwxr-x  7 hadoop hadoop 4.0K Dec 25  2014 neutron-vpnaas
drwxrwxr-x  9 hadoop hadoop 4.0K Dec 24  2014 nova
drwxrwxr-x  9 hadoop hadoop 4.0K Dec 25  2014 noVNC
drwxrwxr-x  5 hadoop hadoop 4.0K Dec 25  2014 requirements
drwxrwxr-x  9 hadoop hadoop 4.0K Dec 25  2014 swift
drwxrwxr-x 10 hadoop hadoop 4.0K Dec 25  2014 trove
```

切换到icehouse分支

```
cd devstack
git branch -a
git checkout -b origin/stable/icehouse
git branch -a
```

如果是使用oh-my-zsh可以方便地看出当前所在的分支. 不使用zsh,则git branch -a绿色的为选中的分支  

```
➜  openstack  cd devstack 
➜  devstack git:(origin/stable/icehouse) 
```

问题1: 安装过程如果下载某个github上的包很慢, 可以手动杀死git进程
```
ps -ef | grep stack
```

问题2: mysql没有权限

```
stack@ubuntu:~/devstack$ mysql -uroot -popenstack -h127.0.0.1
ERROR 1045 (28000): Access denied for user 'root'@'localhost' (using password: YES)
```

解决办法见 http://alsww.blog.51cto.com/2001924/1121676  的方法二

```
stack@ubuntu:~$ sudo cat /etc/mysql/debian.cnf
# Automatically generated for Debian scripts. DO NOT TOUCH!
[client]
host     = localhost
user     = debian-sys-maint
password = ZP50982m3bNcCSYl
socket   = /var/run/mysqld/mysqld.sock
[mysql_upgrade]
host     = localhost
user     = debian-sys-maint
password = ZP50982m3bNcCSYl
socket   = /var/run/mysqld/mysqld.sock
basedir  = /usr
stack@ubuntu:~$ mysql -udebian-sys-maint -p
Enter password: 
Welcome to the MySQL monitor.  Commands end with ; or \g.
mysql> use mysql;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A
Database changed
mysql> UPDATE user SET Password=PASSWORD('openstack') where USER='root'; 
Query OK, 4 rows affected (0.03 sec)
Rows matched: 4  Changed: 4  Warnings: 0
mysql> FLUSH PRIVILEGES; 
Query OK, 0 rows affected (0.00 sec)
mysql> quit 
stack@ubuntu:~$ mysql -uroot -p
Enter password: 
Welcome to the MySQL monitor.  Commands end with ; or \g.
```

问题3: F19-x86_64-cfntools.qcow2无法找到. 在local.conf中注释掉!

问题4: 手动下载几个qcow2包, 包括mysql.qcow2等

http://ams2.mirrors.digitalocean.com/fedora-alt/openstack/20/x86_64/Fedora-x86_64-20-20140618-sda.qcow2  
http://archive.fedoraproject.org/pub/alt/openstack/20/x86_64/Fedora-x86_64-20-20140618-sda.qcow2  

最后成功启动显示下面的信息:

![](http://7xjs7x.com1.z0.glb.clouddn.com/devstack1.png)


<http://192.168.56.30> 用户名/密码: admin/openstack

![](http://7xjs7x.com1.z0.glb.clouddn.com/devstack2.png)

![](http://7xjs7x.com1.z0.glb.clouddn.com/devstack3.png)

