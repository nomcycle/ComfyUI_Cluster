FROM ubuntu:24.10

LABEL version="0.1" \
      description="ComfyUI development using VSCode remote development over tailscale." \
      tag="latest"

ENV DEBIAN_FRONTEND=noninteractive \
    SHELL=/bin/bash
WORKDIR /workspace

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        software-properties-common \
        python3-apt && \
    apt-get update -y && \
    apt-get install -y --no-install-recommends \
        wget \
        git \
        sudo \
        openssh-server \
        curl \
        iptables \
        gnupg \
        tzdata \
        unzip \
        libegl1-mesa-dev \
        vim \
        expect \
        ca-certificates \
        build-essential \
        lsb-release \
        bash && \
    ln -s /usr/lib/python3/dist-packages/apt_pkg.cpython-*-x86_64-linux-gnu.so /usr/lib/python3/dist-packages/apt_pkg.so && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    echo "en_US.UTF-8 UTF-8" > /etc/locale.gen

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN useradd -m -s /bin/bash comfy && \
    echo "comfy ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers && \
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication no/' /etc/ssh/sshd_config && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin no/' /etc/ssh/sshd_config && \
    mkdir -p /home/comfy/.ssh && \
    chown -R comfy:comfy /home/comfy/.ssh && \
    chmod 700 /home/comfy/.ssh && \
    mkdir -p /home/comfy/startup

COPY ./ /workspace/custom_nodes/
RUN ls /workspace/custom_nodes/ && sleep 2
COPY ./ComfyUI_Cluster/test_cluster/container/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Setup workspace permissions
VOLUME /workpace
RUN chown -R comfy:comfy /workspace

ENTRYPOINT ["/entrypoint.sh"]
CMD ["bash"]