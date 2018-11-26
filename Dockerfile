FROM scratch

COPY script/ca-certificates.crt /etc/ssl/certs/
COPY bin/nomad-logzio /

EXPOSE 80
ENTRYPOINT ["/nomad-logzio"]