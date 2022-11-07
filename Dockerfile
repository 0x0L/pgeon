FROM mambaorg/micromamba:1.0.0

COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yml /tmp/environment.yml

RUN micromamba install -n base -y -f /tmp/environment.yml \
 && micromamba clean --all --yes
