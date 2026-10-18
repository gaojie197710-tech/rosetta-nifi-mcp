#!/usr/bin/env bash
# Finishes NiFi <-> Authelia SSO: creates the "nifi" OIDC client secret, writes its digest for
# Authelia and the plaintext into nifi.properties, trusts the Authelia CA in NiFi's truststore,
# then restarts Authelia and NiFi. Run it yourself; the secret is never printed.
set -euo pipefail

# NIFI_HOME: the NiFi install; AUTHELIA_DATA: Authelia's data dir (secrets/, tls/ca.crt);
# AUTHELIA_CONTAINER: the Authelia docker container; AUTHELIA_URL: its base URL.
NIFI="${NIFI_HOME:?set NIFI_HOME to the NiFi install directory}"
AUTHELIA_DATA="${AUTHELIA_DATA:?set AUTHELIA_DATA to the Authelia data directory}"
AUTHELIA="${AUTHELIA_CONTAINER:-authelia}"
AUTHELIA_URL="${AUTHELIA_URL:-https://127.0.0.1:9091}"
PROPS="$NIFI/conf/nifi.properties"
DIGEST="$AUTHELIA_DATA/secrets/client_nifi.digest"

secret=$(openssl rand -hex 32)

digest=$(docker exec "$AUTHELIA" authelia crypto hash generate pbkdf2 --variant sha512 --password "$secret" | sed -n 's/^Digest: //p')
[ -n "$digest" ] || { echo "Could not generate the digest; nothing changed."; exit 1; }
umask 077; printf '%s' "$digest" > "$DIGEST"

sed -i '' "s#^nifi.security.user.oidc.client.secret=.*#nifi.security.user.oidc.client.secret=${secret}#" "$PROPS"
unset secret digest

tspass=$(sed -n 's/^nifi.security.truststorePasswd=//p' "$PROPS")
if ! keytool -list -keystore "$NIFI/conf/truststore.p12" -storetype PKCS12 -storepass "$tspass" -alias authelia-ca >/dev/null 2>&1; then
  keytool -importcert -noprompt -alias authelia-ca -file "$AUTHELIA_DATA/tls/ca.crt" \
    -keystore "$NIFI/conf/truststore.p12" -storetype PKCS12 -storepass "$tspass"
fi
unset tspass

docker restart "$AUTHELIA" >/dev/null
sleep 5
docker logs --since 20s "$AUTHELIA" 2>&1 | grep -iE 'error|fatal' && { echo "Authelia reported errors (above)."; exit 1; }
curl -sfk "$AUTHELIA_URL"/.well-known/openid-configuration >/dev/null && echo "Authelia OK"

"$NIFI/bin/nifi.sh" restart
echo "NiFi restarting; open https://localhost:9443/nifi in about a minute."
