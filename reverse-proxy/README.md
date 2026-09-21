# Certificat du reverse proxy

`certs/pipeline.crt` et `certs/pipeline.key` — **jamais versionnés** (voir `.gitignore`). Auto-signés,
régénérables à tout moment sans casser quoi que ce soit d'autre.

## Régénérer

```powershell
cd C:\Users\charles.nguessan\Documents\pipeline_temps_reel
podman run --rm -v "${PWD}\reverse-proxy\certs:/certs" docker.io/library/alpine:3.20 sh -c "
  apk add --no-cache openssl >/dev/null 2>&1
  openssl req -x509 -newkey rsa:2048 -nodes -days 825 \
    -keyout /certs/pipeline.key -out /certs/pipeline.crt \
    -subj '/C=CI/O=CNAM-CI/OU=SGD/CN=pipeline-dprest.local' \
    -addext 'subjectAltName=DNS:pipeline-dprest.local,DNS:localhost,IP:127.0.0.1'
  chmod 644 /certs/pipeline.key /certs/pipeline.crt
"
podman compose up -d --force-recreate reverse-proxy
```

## Pourquoi auto-signé, et pas un certificat reconnu

Ce simulateur tourne sur une machine locale, jamais exposée à Internet. Un certificat auto-signé
suffit à démontrer le chiffrement — le navigateur affiche un avertissement au premier accès (normal,
voir `docs/guides/etape7_securite.md`), à accepter manuellement une fois.

En production, ce certificat serait remplacé par un certificat émis par une autorité reconnue (interne
à la CNAM, ou un fournisseur reconnu par l'ARTCI).
