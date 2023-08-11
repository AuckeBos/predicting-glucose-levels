FROM python:3.8-slim
LABEL maintainer="Aucke Bos"

# Set user
ARG USERNAME=glucose
ARG UID=1000
ARG GID=$UID
RUN apt update && \
    apt install -y sudo && \
    addgroup --gid $GID glucose && \
    adduser --uid $UID --gid $GID --disabled-password --gecos "" glucose && \
    echo 'glucose ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers
RUN chmod -R 755 /home/$USERNAME

# Install poetry
RUN pip install poetry
# Copy files
COPY --chown=glucose:glucose ./pyproject.toml ./poetry.lock ./docker/entrypoint.sh /home/glucose/
COPY --chown=glucose:glucose ./predicting_glucose_levels /home/glucose/predicting_glucose_levels    

# Install deps
WORKDIR /home/glucose
RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi

USER $USERNAME
# Todo: Use env var username
# ENTRYPOINT /home/glucose/entrypoint.sh

# ENTRYPOINT ["/bin/sh", "-c", "/home/glucose/entrypoint.sh"]