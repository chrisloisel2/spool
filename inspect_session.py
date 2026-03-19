import os
import sys
import posixpath
import paramiko

SFTP_CONFIG = {
    "host": "192.168.88.121",
    "port": 22,
    "user": "spool",
    "password": "root",
    "inbox_base": "/srv/exoria/inbox",
}

import paramiko

def connect_sftp():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname="192.168.88.121",
        port=22,
        username="spool",
        password="root",
        timeout=20,
        banner_timeout=20,
        auth_timeout=20,
        look_for_keys=False,
        allow_agent=False,
    )
    sftp = client.open_sftp()
    return client, sftp

def mkdir_p(sftp, remote_path):
    parts = remote_path.strip("/").split("/")
    current = "/"
    for part in parts:
        current = posixpath.join(current, part)
        try:
            sftp.stat(current)
        except FileNotFoundError:
            sftp.mkdir(current)

def upload_directory(local_dir, remote_dir):
    transport = None
    sftp = None

    try:
        transport, sftp = connect_sftp()
        print("Connexion SFTP OK")

        for root, _, files in os.walk(local_dir):
            rel_path = os.path.relpath(root, local_dir)
            if rel_path == ".":
                target_dir = remote_dir
            else:
                target_dir = posixpath.join(remote_dir, rel_path)

            mkdir_p(sftp, target_dir)

            for name in files:
                local_file = os.path.join(root, name)
                remote_file = posixpath.join(target_dir, name)
                print(f"Upload {local_file} -> {remote_file}")
                sftp.put(local_file, remote_file)

        print("Upload terminé")

    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: python3 {sys.argv[0]} <dossier_local>")
        sys.exit(1)

    local_folder = sys.argv[1]
    remote_folder = SFTP_CONFIG["inbox_base"]

    if not os.path.isdir(local_folder):
        print(f"Dossier introuvable: {local_folder}")
        sys.exit(1)

    upload_directory(local_folder, remote_folder)
