FROM centos:latest

LABEL maintainer="simon.aubury@iag.com.au"
LABEL version="2.0"
LABEL description="The IAG data_pipleline"

RUN set -ex; \
    update-ca-trust ;\
    curl -O https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm ;\
    rpm -i epel-release-latest-7.noarch.rpm ;\
    yum update -y 
RUN yum install -y git 
RUN yum install -y python 
RUN yum install -y make 
RUN yum install -y gcc 
RUN yum install -y wget 
RUN yum install -y python-devel 
RUN yum install -y python-wheel 
RUN yum install -y python-pip 
RUN yum install -y libaio 
RUN yum install -y librdkafka-devel 
#RUN yum install -y build-dep 
RUN yum install -y python-psycopg2 
RUN yum install -y python-cffi 
RUN yum install -y libffi-devel 
RUN yum install -y openssl-devel 
RUN yum install -y postgresql  
RUN yum install -y python-cffi 
RUN yum install -y libffi-devel 
RUN yum install -y openssl-devel 
RUN pip install cx_oracle
RUN pip install pytest

COPY ./rpm_tmp /tmp
RUN rpm -ivh /tmp/oracle-instantclient12.2-basic-12.2.0.1.0-1.x86_64.rpm
RUN rpm -ivh /tmp/oracle-instantclient12.2-devel-12.2.0.1.0-1.x86_64.rpm

RUN mkdir /usr/local/data_pipeline

# I know you are not supposed to do this
ENTRYPOINT ["tail", "-f", "/dev/null"]
