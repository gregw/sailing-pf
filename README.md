# Australian Yacht Racing Elapsed Time Database

The Performance Factor (**PF**) project provides an online database of **elapsed times** and
Time Corrected Factors (**TCF**s) for Australian keelboat racing, with statistical analysis and graphical
presentation. Fundamentally, this project is about presenting the ultimate source of truth - **Elapsed Times**, in a form that
can better inform a club handicappers' decisions about handicap allocation. The Performance Factor(**PF**),
Reference Factor(**RF**), and Performance Profile(**PP**) are just alternate (experimental) ways of
presenting the relationships between the elapsed times of various boats and designs.

A **PF** is a back-calculated Time Correction Factor (**TCF**) a boat would have needed, averaged across
all its recorded racing history, to have been near equal corrected time with a hypothetical 1.000 reference
boat. An **RF** is a similar **TCF** for boat designs. The **PP** is a totally new way of presenting
a boat's historical performance.

It is a historical performance measure, not a handicap system,
primarily used to inform initial handicap allocation at the start of a season.

For more information see [`src/main/resources/content/docs.md`](https://github.com/gregw/sailing-pf/blob/main/src/main/resources/content/docs.md).

Source code: Apache License 2.0. Contributions and issue reports welcome via
[GitHub Issues](https://github.com/gregw/sailing-pf/issues).

## Getting Started

This is a Java 21 / Maven project. The only dependencies are a JDK (21 or later)
and Maven — Maven downloads everything else on the first build.

### 1. Install a JDK and Maven

**Linux (Debian/Ubuntu)**

```bash
sudo apt update
sudo apt install default-jdk maven git
```

**macOS** (with [Homebrew](https://brew.sh))

```bash
brew install openjdk maven git
```

**Windows**

Install [Git for Windows](https://git-scm.com/download/win), a JDK such as
[Temurin 21](https://adoptium.net/temurin/releases/?version=21), and
[Maven](https://maven.apache.org/download.cgi) (add its `bin` directory to your
`PATH`). The commands below then work from PowerShell or Git Bash.

Verify the tools are on your `PATH`:

```bash
java -version    # should report 21 or later
mvn -version
```

### 2. Get the code and build

```bash
git clone https://github.com/gregw/sailing-pf.git
cd sailing-pf
mvn compile
```

### 3. Populate the database

The project reads and writes a data directory (referred to as `pf-data`). To
build one from scratch, run the `FullImport` class — it runs every importer in
turn (SailSys, TopYacht, BWPS, ORC, AMS) against a single data directory:

```bash
mvn exec:java -Dexec.mainClass=org.mortbay.sailing.pf.importer.FullImport -Dpf-data=pf-data
```

This fetches race data over the network and may take a while. The data
directory is created if it does not exist; re-running updates it in place.

### 4. Run the server

```bash
mvn exec:java -Dpf-data=pf-data
```

Then open the public UI at <http://localhost:8080> and the admin UI at
<http://localhost:8888>.

## Running locally

```bash
mvn exec:java -Dpf-data=/path/to/pf-data
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
git clone https://github.com/gregw/sailing-pf.git
cd sailing-pf
sudo bash etc/install.sh
```

The script will:
- Create a `sailing-pf` system user
- Copy the project to `/opt/sailing-pf`
- Create a data directory at `/var/lib/sailing-pf`
- Compile the project
- Install and enable the systemd service

To pre-populate with existing data, copy your `pf-data/` contents into `/var/lib/sailing-pf/`
before starting the service.

### Service management

```bash
sudo systemctl start sailing-pf     # start the server
sudo systemctl stop sailing-pf      # stop the server
sudo systemctl restart sailing-pf   # restart after a code update
sudo systemctl status sailing-pf    # check status
sudo journalctl -u sailing-pf -f    # tail the logs
```

The service is configured to start automatically at boot (`WantedBy=multi-user.target`).
To disable autostart: `sudo systemctl disable sailing-pf`.

### Updating

After pulling new code, rebuild and restart:

```bash
cd /opt/sailing-pf
sudo -u sailing-pf mvn compile -q
sudo systemctl restart sailing-pf
```
