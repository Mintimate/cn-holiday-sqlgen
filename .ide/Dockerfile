# .ide/Dockerfile
FROM python:3.12.11-bookworm

# 安装 ssh 服务(用于支持 VSCode 客户端通过 Remote-SSH 访问开发环境);
RUN apt update && \
    apt install -y \
    git git-lfs \
    webp \
    wget \
    unzip \
    openssh-server \
    vim \
    zsh \
    zip \
    jq \
    libxml2-utils && \
    chsh -s /bin/zsh && \
    yes | zsh -c "$(curl -fsSL 'https://cnb.cool/Mintimate/tool-forge/oh-my-zsh/-/git/raw/main/AutoInstallZshWithPlugin.sh')"

# 安装 code-server 和 vscode 常用插件
RUN curl -fsSL https://code-server.dev/install.sh | sh \
  && code-server --install-extension redhat.vscode-yaml \
  && code-server --install-extension dbaeumer.vscode-eslint \
  && code-server --install-extension eamodio.gitlens \
  && code-server --install-extension ms-python.python \
  && echo done

# 设置全局 pip 源
RUN pip3 config set global.index-url https://mirrors.cloud.tencent.com/pypi/simple

# 安装字体
RUN wget https://github.com/lxgw/LxgwWenKai/releases/download/v1.520/lxgw-wenkai-v1.520.zip && \
    unzip lxgw-wenkai-v1.520.zip && \
    mv lxgw-wenkai-v1.520 /usr/share/fonts/truetype && \
    rm lxgw-wenkai-v1.520.zip && \
    fc-cache -fv && \
    echo "done"

# 指定字符集支持命令行输入中文（根据需要选择字符集）
ENV LANG zh_CN.UTF-8
ENV LC_ALL zh_CN.UTF-8