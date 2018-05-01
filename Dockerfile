FROM robsyme/docker-sge


# Install python/pyenv dependencies and build tools
RUN if getent hosts apt_cacher_ng; then \
      export http_proxy=http://apt_cacher_ng:3142/; \
    fi; \
    apt-get update -y \
    &&  apt-get install -y build-essential git libssl-dev zlib1g-dev libbz2-dev \
                libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev \
                xz-utils tk-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install pyenv
RUN su - sgeuser bash -lc 'curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash'
RUN printf 'PYENV_ROOT=~/.pyenv\nexport PATH=$PYENV_ROOT/bin:$PATH\neval "$(pyenv init -)"' >> /home/sgeuser/.profile
RUN su - sgeuser bash -c "pyenv install 3.6.5"
RUN su - sgeuser bash -c "pyenv install 3.5.5"
RUN su - sgeuser bash -c "pyenv install 3.4.8"
RUN su - sgeuser bash -c "pyenv install 2.7.14"
RUN su - sgeuser bash -c "pyenv global 3.6.5 3.5.5 3.4.8 2.7.14 && pip3 install --upgrade pip tox"
# Fix DRMAA settings
RUN sed -if /etc/profile.d/sge_settings.sh -e "s;DRMAA_LIBRARY_PATH=/opt/sge/lib//libdrmaa.so;export DRMAA_LIBRARY_PATH=/opt/sge/lib/lx-amd64/libdrmaa.so;"

# RUN echo 'export DRMAA_LIBRARY_PATH=/opt/sge/lib/lx-amd64/libdrmaa.so' >> /etc/profile.d/drmaa.sh

# eval "$(pyenv init -)"
# eval "$(pyenv virtualenv-init -)"
# USER root
