#参考自https://zhuanlan.zhihu.com/p/38552260
FROM hpy253215039/python3.6:1.0.0

LABEL maintainer="hpy253215039@163.com"

COPY ./src/ /rocketmq-exporter/src/

WORKDIR /rocketmq-exporter/src

EXPOSE 9104

#CMD []
ENTRYPOINT python rocketmq_exporter.py > /rocketmq-exporter/rocketmq-exporter.log

