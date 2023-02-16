FROM --platform=linux/amd64 python:3.10-slim

# https://github.com/mozilla-services/Dockerflow/blob/master/docs/building-container.md
ARG USER_ID="10001"
ARG GROUP_ID="app"
ARG HOME="/app"

ENV HOME=${HOME}
RUN groupadd --gid ${USER_ID} ${GROUP_ID} && \
    useradd --create-home --uid ${USER_ID} --gid ${GROUP_ID} --home-dir ${HOME} ${GROUP_ID}

WORKDIR ${HOME}

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-deps --no-cache-dir -r requirements.txt \
    && rm requirements.txt

COPY . sync_src

RUN pip install --no-dependencies --no-cache-dir -e sync_src

RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}
USER ${USER_ID}

ENTRYPOINT ["datahub-sync"]