FROM python:3.8-slim
LABEL maintainer="Aucke Bos"

# Set user
ARG USERNAME=glucose
ARG UID=1000
ARG GID=$UID
RUN apt update && \
    apt install -y sudo && \
    addgroup --gid $GID glucose && \
    adduser --uid $UID --gid $GID --disabled-password --gecos "" ${USERNAME} && \
    echo '${USERNAME} ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers
RUN apt install -y gcc
RUN chmod -R 755 /home/$USERNAME

# Install poetry
RUN pip install poetry
# Copy files
COPY --chown=${USERNAME}:${USERNAME} ./pyproject.toml ./poetry.lock ./docker/entrypoint.sh /home/${USERNAME}/
COPY --chown=${USERNAME}:${USERNAME} ./predicting_glucose_levels /home/${USERNAME}/predicting_glucose_levels    
RUN chmod +x /home/${USERNAME}/entrypoint.sh

# Install deps
WORKDIR /home/${USERNAME}
RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi

USER $USERNAME
ENV USERNAME=${USERNAME}
ENTRYPOINT ["/bin/sh", "-c", "/home/${USERNAME}/entrypoint.sh"]