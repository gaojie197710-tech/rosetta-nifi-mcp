#!/usr/bin/env bash
# Gives the NiFi MCP server its own login: a local CA and a client certificate (CN=nifi-mcp)
# under ~/.nifi-mcp, the CA trusted by NiFi, and the identity granted the admin's policies plus
# provenance, system diagnostics and root data access. Dev instance only. Secrets are never printed.
set -euo pipefail

NIFI="${NIFI_HOME:?set NIFI_HOME to the NiFi install directory}"
CONF="$NIFI/conf"
OUT=~/.nifi-mcp
IDENTITY="CN=nifi-mcp"

mkdir -p "$OUT"; chmod 700 "$OUT"; umask 077
cd "$OUT"

if [ ! -f ca.key ]; then
  openssl req -x509 -newkey rsa:3072 -sha256 -days 3650 -nodes -keyout ca.key -out ca.crt \
    -subj "/CN=nifi-mcp-local-ca" -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
fi
if [ ! -f client.key ]; then
  openssl req -newkey rsa:3072 -sha256 -nodes -keyout client.key -out client.csr -subj "/CN=nifi-mcp" 2>/dev/null
  printf 'basicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=clientAuth\n' > client.ext
  openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 825 -sha256 \
    -extfile client.ext -out client.crt 2>/dev/null
  rm -f client.csr client.ext
fi

# NiFi's own server certificate, so the MCP client can verify https://localhost:9443.
openssl s_client -connect localhost:9443 -servername localhost -showcerts </dev/null 2>/dev/null \
  | awk '/BEGIN CERTIFICATE/,/END CERTIFICATE/' > nifi-server-chain.pem
umask 022; chmod 644 ca.crt client.crt nifi-server-chain.pem

tspass=$(sed -n 's/^nifi.security.truststorePasswd=//p' "$CONF/nifi.properties")
if ! keytool -list -keystore "$CONF/truststore.p12" -storetype PKCS12 -storepass "$tspass" -alias nifi-mcp-ca >/dev/null 2>&1; then
  keytool -importcert -noprompt -alias nifi-mcp-ca -file "$OUT/ca.crt" \
    -keystore "$CONF/truststore.p12" -storetype PKCS12 -storepass "$tspass"
fi
unset tspass

"$NIFI/bin/nifi.sh" stop

TS=$(date +%Y%m%d%H%M%S)
cp "$CONF/users.xml" "$CONF/users.xml.bak-before-mcp-$TS"
cp "$CONF/authorizations.xml" "$CONF/authorizations.xml.bak-before-mcp-$TS"

python3 -I - "$CONF" "$IDENTITY" <<'PY'
import sys, uuid, xml.etree.ElementTree as ET
conf, identity = sys.argv[1], sys.argv[2]
ROOT_PG = "6bffa765-01a0-1000-7097-0440fe166ba4"
ADMIN = "jie.gao@tdsynnex.com"

ut = ET.parse(f"{conf}/users.xml"); users = ut.getroot().find("users")
by_identity = {u.get("identity"): u.get("identifier") for u in users}
admin_id = by_identity[ADMIN]
mcp_id = by_identity.get(identity) or str(uuid.uuid5(uuid.NAMESPACE_URL, identity))
if identity not in by_identity:
    ET.SubElement(users, "user", identifier=mcp_id, identity=identity)
ut.write(f"{conf}/users.xml", encoding="UTF-8", xml_declaration=True)

at = ET.parse(f"{conf}/authorizations.xml"); policies = at.getroot().find("policies")
extra = [("/provenance", "R"), ("/system", "R"),
         (f"/provenance-data/process-groups/{ROOT_PG}", "R")]
existing = {(p.get("resource"), p.get("action")): p for p in policies}
for res, act in extra:
    if (res, act) not in existing:
        p = ET.SubElement(policies, "policy", identifier=str(uuid.uuid5(uuid.NAMESPACE_URL, res + act)),
                          resource=res, action=act)
        ET.SubElement(p, "user", identifier=admin_id)
        existing[(res, act)] = p
for p in policies:
    ids = {u.get("identifier") for u in p.findall("user")}
    if admin_id in ids and mcp_id not in ids:
        ET.SubElement(p, "user", identifier=mcp_id)
ET.indent(at); at.write(f"{conf}/authorizations.xml", encoding="UTF-8", xml_declaration=True)
print(f"granted {identity} on {sum(1 for p in policies if any(u.get('identifier')==mcp_id for u in p))} policies")
PY

"$NIFI/bin/nifi.sh" start
for i in $(seq 1 40); do
  code=$(curl -s -o /dev/null -w '%{http_code}' --cacert "$OUT/nifi-server-chain.pem" \
    --cert "$OUT/client.crt" --key "$OUT/client.key" https://localhost:9443/nifi-api/flow/current-user || true)
  [ "$code" = "200" ] && break; sleep 3
done
curl -s --cacert "$OUT/nifi-server-chain.pem" --cert "$OUT/client.crt" --key "$OUT/client.key" \
  https://localhost:9443/nifi-api/flow/current-user | python3 -c 'import json,sys; d=json.load(sys.stdin); print("identity:", d["identity"], "| anonymous:", d["anonymous"])'
