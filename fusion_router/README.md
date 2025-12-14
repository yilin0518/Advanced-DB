Fusion-router混合式数据库部署和测试程序

首先使用docker build启用mysql和mongoDB docker端口

目前我使用docker+本地程序测试的方式,如果想直接使用docker运行,请使用fusion_router里的Dockerfile启用api/app.py服务,并且可能需要更改app.py的URL字段.

本地测试:启用docker的容器服务后,进入fusion_router文件夹下,使用
```Bash
python api/app.py
```
启用fusion-router端口服务.这一过程第一次运行时包含两个数据库的写入,可能需要等待一段时间.
然后,运行test_debug.py以检查服务的运行状态,运行test_fusion.py以在混合模式DB上实现对于之前使用同款测试样例的测试.
