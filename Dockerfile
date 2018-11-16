FROM scratch

COPY script/ca-certificates.crt /etc/ssl/certs/
COPY dist/tent /

EXPOSE 80
ENTRYPOINT ["/tent"]