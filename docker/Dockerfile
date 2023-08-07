FROM python:3.8-slim
LABEL maintainer="Aucke Bos"

# Set user
ARG USERNAME=glucose
ARG UID=1000
ARG GID=$UID
RUN apt update && \
    apt install -y sudo && \
    addgroup --gid $GID ${USERNAME} && \
    adduser --uid $UID --gid $GID --disabled-password --gecos "" ${USERNAME} && \
    echo '${USERNAME} ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers
RUN chmod -R 755 /home/$USERNAME

# Install poetry
RUN pip install poetry

# Copy files
COPY --chown=${USERNAME}:${USERNAME} predicting_glucose_levels /home/${USERNAME}/predicting_glucose_levels
COPY --chown=${USERNAME}:${USERNAME} pyproject.toml poetry.lock /home/${USERNAME}/

# Install deps
WORKDIR /home/${USERNAME}

RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi

USER $USERNAME
# Temp set entrypoint
ENTRYPOINT ["tail", "-f", "/dev/null"]