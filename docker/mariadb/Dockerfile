FROM mariadb
RUN echo 'skip-name-resolve' >> /etc/mysql/conf.d/mariadb.cnf
ENV MYSQL_ROOT_PASSWORD=EcsSync!
ENV MYSQL_USER=ecssync
ENV MYSQL_PASSWORD=ecs-sync-db
ENV MYSQL_DATABASE=ecs_sync
