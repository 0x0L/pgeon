# FROM mambaorg/micromamba:1.3.1
# COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yml /tmp/environment.yml
# RUN micromamba install -y -n base -f /tmp/environment.yml && \
#     micromamba clean --all --yes

FROM python:3.11
