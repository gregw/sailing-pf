# Australian Yacht Racing Elapsed Time Database

An online database of elapsed times from Australian yacht racing that computes a
**Historical Performance Factor (HPF)** — the back-calculated time correction factor a boat
would have needed, averaged across its recorded racing history, to have been equal-time with a
hypothetical 1.000 reference boat. It is a historical performance measure, not a handicap system,
primarily used to inform initial handicap allocation at the start of a season.

For more information see [`src/main/resources/content/docs.md`](https://github.com/gregw/sailing-hpf/blob/main/src/main/resources/content/docs.md).

Source code: Apache License 2.0. Contributions and issue reports welcome via
[GitHub Issues](https://github.com/gregw/sailing-hpf/issues).

## Running locally

```bash
mvn exec:java -Dhpf-data=/path/to/hpf-data
```

The admin UI is served on port 8888 and the public UI on port 8080 by default.

## Installing as a systemd service (Raspberry Pi / Debian)

The `etc/` directory contains a systemd unit file and an install script for running the server
as a background service on Debian-based systems (tested on Raspberry Pi OS on a Pi 5).

### Prerequisites

```bash
sudo apt update
sudo apt install default-jdk maven
```

### Install

Clone the repository, then run the install script as root from the project directory:

```bash
git clone https://github.com/gregw/sailing-hpf.git
cd sailing-hpf
sudo bash etc/install.sh
```

The script will:
- Create a `sailing-hpf` system user
- Copy the project to `/opt/sailing-hpf`
- Create a data directory at `/var/lib/sailing-hpf`
- Compile the project
- Install and enable the systemd service

To pre-populate with existing data, copy your `hpf-data/` contents into `/var/lib/sailing-hpf/`
before starting the service.

### Service management

```bash
sudo systemctl start sailing-hpf     # start the server
sudo systemctl stop sailing-hpf      # stop the server
sudo systemctl restart sailing-hpf   # restart after a code update
sudo systemctl status sailing-hpf    # check status
sudo journalctl -u sailing-hpf -f    # tail the logs
```

The service is configured to start automatically at boot (`WantedBy=multi-user.target`).
To disable autostart: `sudo systemctl disable sailing-hpf`.

### Updating

After pulling new code, rebuild and restart:

```bash
cd /opt/sailing-hpf
sudo -u sailing-hpf mvn compile -q
sudo systemctl restart sailing-hpf
```
