FROM ubuntu
MAINTAINER gijs@pythonic.nl
ENV DEBIAN_FRONTEND noninteractive

## install requirements
RUN apt-get update
RUN apt-get install -y curl erlang git make python openssh-server supervisor

## add user for disco
RUN adduser --system disco --shell /bin/sh

# setup ssh
RUN echo root:disco | chpasswd
RUN mkdir -p /var/run/sshd

## passwordless login for docker 
RUN mkdir -p /home/disco/.ssh
RUN ssh-keygen -N '' -f /home/disco/.ssh/id_dsa
RUN cat /home/disco/.ssh/id_dsa.pub >> /home/disco/.ssh/authorized_keys
RUN echo -n "localhost " > /home/disco/.ssh/known_hosts
RUN cat /etc/ssh/ssh_host_rsa_key.pub >> /home/disco/.ssh/known_hosts
RUN chown disco -R /home/disco/.ssh

## install disco
RUN git clone https://github.com/discoproject/disco.git /disco
RUN cd /disco &&  make install
RUN cd /disco/lib && python setup.py install
RUN chown -R disco /usr/var/disco

## configure the supervisor which will start ssh & docker
ADD supervisor.conf /etc/supervisor/conf.d/disco.conf
RUN sed -i 's/without-password/yes/g' /etc/ssh/sshd_config

EXPOSE 22
EXPOSE 8990
EXPOSE 8989
CMD ["/usr/bin/supervisord"]

