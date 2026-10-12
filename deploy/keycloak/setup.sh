#!/bin/sh
set -eu
until /opt/keycloak/bin/kcadm.sh config credentials \
  --server http://keycloak:8080 \
  --realm master \
  --user "${KC_BOOTSTRAP_ADMIN_USERNAME}" \
  --password "${KC_BOOTSTRAP_ADMIN_PASSWORD}"; do
  sleep 3
done

until /opt/keycloak/bin/kcadm.sh get realms/jungle >/dev/null 2>&1; do
  sleep 2
done

assign() {
  /opt/keycloak/bin/kcadm.sh add-roles -r jungle --uusername "service-account-$1" --rolename "$2" \
    || /opt/keycloak/bin/kcadm.sh get-roles -r jungle --uusername "service-account-$1" | grep -q "\"$2\""
}

assign provider-a provider
assign provider-b provider
assign provider-short provider
assign wallet-internal internal
echo "keycloak clients are ready"
