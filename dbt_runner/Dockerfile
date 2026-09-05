FROM public.ecr.aws/lambda/python:3.13

COPY dbt_runner/requirements.txt /tmp/requirements.txt
# gcc: mmh3 (dbt dependency) ships no arm64 wheel for Python 3.13.
# It stays installed: removing it breaks annobin-plugin-gcc in this base.
RUN dnf install -y gcc && dnf clean all && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Code only: the dbt project and its secret-free profiles.yml. Reference
# data is never baked into images (repo convention).
COPY dbt/ /var/task/dbt/
# Resolve dbt packages (dbt_utils) at build time, pinned by package-lock.yml.
# The local dbt_packages/ dir is dockerignored: the image never depends on
# whatever the dev machine has installed.
RUN cd /var/task/dbt && dbt deps

COPY dbt_runner/__init__.py dbt_runner/handler.py /var/task/dbt_runner/

CMD ["dbt_runner.handler.handler"]
