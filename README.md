## python-docker-image

以官方镜像python3.6为基准安装prometheus-client, flask等必要组件。

cd python-docker-image

sh ./docker-python3.6.build.sh

## hpy-rocketmq-exporter

以前述镜像为基准制作rocketmq-exporter镜像。

sh ./docker.build.sh

## 一些瑕疵

1. 目前只支持rocketmq-console的变量设置。

2.数据校验方面有遗漏，必须存在至少一个topic和consumer才正常，否则json解析失败，metrics获取失败。
