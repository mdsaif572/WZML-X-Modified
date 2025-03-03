FROM mysterysd/wzmlx:hkwzv3

WORKDIR /usr/src/app
RUN chmod 777 /usr/src/app

RUN uv venv

COPY requirements.txt .
COPY megasdk-4.8.0-py2.py3-none-any.whl .
RUN uv pip install --upgrade pip setuptools
RUN uv pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["bash", "start.sh"]
