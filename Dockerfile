FROM scratch

COPY script/ca-certificates.crt /etc/ssl/certs/
COPY dist/nomad-logzio /

EXPOSE 80
ENTRYPOINT ["/tent"]