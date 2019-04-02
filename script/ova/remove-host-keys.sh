#!/usr/bin/env bash
if [ "$(id -u)" != "0" ]; then
    echo "Please run as root"
    exit 1
fi

echo '---WARNING---'
echo 'Resetting SSH host keys!'

pushd /etc/ssh > /dev/null
if tar czf host-keys-$(date -I).tgz ssh_host_*; then
  rm ssh_host_*
  echo 'Done'
fi
popd > /dev/null