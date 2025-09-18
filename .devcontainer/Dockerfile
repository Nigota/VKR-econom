FROM python:3.12.11-bullseye

# Установка зависимостей системы и локалей
RUN apt-get update && apt-get install -y \
    git \
    curl \
    sudo \
    build-essential \
    libssl-dev \
    libffi-dev \
    tzdata \
    locales \
    && rm -rf /var/lib/apt/lists/*

# Настройка таймзоны на Москву
RUN ln -fs /usr/share/zoneinfo/Europe/Moscow /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata

# Настройка локали на русский
RUN sed -i 's/# ru_RU.UTF-8 UTF-8/ru_RU.UTF-8 UTF-8/' /etc/locale.gen && \
    locale-gen ru_RU.UTF-8 && \
    update-locale LANG=ru_RU.UTF-8

ENV LANG=ru_RU.UTF-8
ENV LANGUAGE=ru_RU:ru
ENV LC_ALL=ru_RU.UTF-8

# Создание пользователя vscode
RUN useradd -m -s /bin/bash vscode && \
    echo 'vscode ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/vscode && \
    chmod 0440 /etc/sudoers.d/vscode

# Переключение на обычного пользователя
USER vscode

CMD [ "bash" ]