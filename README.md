# Cryptocurrency Reinforcement Learning

## Badges

![Python](https://img.shields.io/badge/python-3.8-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.5-green)

## About
- Build dev pipeline library to pull data
- Pull Cryptocurrency data
- Build RL agent to trade on the cryptocurrency data

```
            - name: Setup Miniconda
  # You may pin to the exact commit or the version.
  # uses: conda-incubator/setup-miniconda@a4260408e20b96e80095f42ff7f1a15b27dd94ca
  uses: conda-incubator/setup-miniconda@v3.0.4
  with:
    # If provided, this installer will be used instead of a miniconda installer, and cached based on its full URL Visit https://github.com/conda/constructor for more information on creating installers
    installer-url: # optional, default is 
    # If provided, this version of Miniconda3 will be downloaded and installed. Visit https://repo.continuum.io/miniconda/ for more information on available versions.
    miniconda-version: # optional, default is 
    # If provided, this variant of Miniforge will be downloaded and installed. If `miniforge-version` is not provided, the `latest` version will be used. Currently-known values: - Miniforge3 (default) - Miniforge-pypy3 - Mambaforge - Mambaforge-pypy3 Visit https://github.com/conda-forge/miniforge/releases/ for more information on available variants
    miniforge-variant: # optional, default is 
    # If provided, this version of the given Miniforge variant will be downloaded and installed. If `miniforge-variant` is not provided, `Miniforge3` will be used. Visit https://github.com/conda-forge/miniforge/releases/ for more information on available versions
    miniforge-version: # optional, default is 
    # Specific version of Conda to install after miniconda is located or installed. See https://anaconda.org/anaconda/conda for available "conda" versions.
    conda-version: # optional, default is 
    # Version of conda build to install. If not provided conda-build is not installed. See https://anaconda.org/anaconda/conda-build for available "conda-build" versions.
    conda-build-version: # optional, default is 
    # Environment.yml to create an environment. See https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-from-an-environment-yml-file for more information.
    environment-file: # optional, default is 
    # Environment name (or path) to activate on all shells. Default is `test` which will be created in `$CONDA/envs/test`. If an empty string is used, no environment is activated by default (For `base` activation see the `auto-activate-base` option). If the environment does not exist, it will be created and activated. If `environment-file` is used and you want that to be the environment used, you need to explicitely provide the name of that environment on `activate-environment`. If using sh/bash/cmd.exe shells please read the IMPORTANT! section on the README.md! to properly activate conda environments on these shells.
    activate-environment: # optional, default is test
    # Exact version of a Python version to use on "activate-environment". If provided, this will be installed before the "environment-file". See https://anaconda.org/anaconda/python for available "python" versions.
    python-version: # optional, default is 
    # Conda configuration. When the channel alias is Anaconda.org or an Anaconda Server GUI, you can set the system configuration so that users automatically see private packages. Anaconda.org was formerly known as binstar.org. This uses the Anaconda command-line client, which you can install with conda install anaconda-client, to automatically add the token to the channel URLs. The default is "true". See https://docs.conda.io/projects/conda/en/latest/user-guide/configuration/use-condarc.html#add-anaconda-org-token-to-automatically-see-private-packages-add-anaconda-token for more information.
    add-anaconda-token: # optional, default is 
    # Conda configuration. Add pip, wheel, and setuptools as dependencies of Python. This ensures that pip, wheel, and setuptools are always installed any time Python is installed. The default is "true". See https://docs.conda.io/projects/conda/en/latest/user-guide/configuration/use-condarc.html#add-pip-as-python-dependency-add-pip-as-python-dependency for more information.
    add-pip-as-python-dependency: # optional, default is 
    # Conda configuration. When allow_softlinks is "true", conda uses hard-links when possible and soft-links---symlinks---when hard-links are not possible, such as when installing on a different file system than the one that the package cache is on. When allow_softlinks is "false", conda still uses hard-links when possible, but when it is not possible, conda copies files. Individual packages can override this option, specifying that certain files should never be soft-linked. The default is "true". See https://docs.conda.io/projects/conda/en/latest/user-guide/configuration/use-condarc.html#disallow-soft-linking-allow-softlinks for more information.
    allow-softlinks: # optional, default is 
    # Conda configuration. If you’d prefer that conda’s base environment not be activated on startup, set the to "false". Default is "true". This setting always overrides if set to "true" or "false". If you want to use the "condarc-file" setting pass and empty string. See https://docs.conda.io/projects/conda/en/latest/user-guide/configuration/ for more information.
    auto-activate-base: # optional, default is true
    # Conda configuration. When "true", conda updates itself any time a user updates or installs a package in the base environment. When "false", conda updates itself only if the user manually issues a conda update command. The default is "false".  This setting always overrides if set to "true" or "false". If you want to use the "condarc-file" setting pass and empty string. See https://docs.conda.io/projects/conda/en/latest/user-guide/configuration/ for more information.
    auto-update-conda: # optional, default is false
    # Conda configuration. Path to a conda configuration file to use for the runner. This file will be copied to "~/.condarc". See https://docs.conda.io/projects/conda/en/latest/user-guide/configuration/ for more information.
    condarc-file: # optional, default is 
    # Conda configuration. Whenever you use the -c or --channel flag to give conda a channel name that is not a URL, conda prepends the channel_alias to the name that it was given. The default channel_alias is https://conda.anaconda.org. See https://docs.conda.io/projects/conda/en/latest/user-guide/configuration/use-condarc.html#set-a-channel-alias-channel-alias for more information.
    channel-alias: # optional, default is 
    # Conda configuration. Accepts values of "strict", "flexible", and "disabled". The default value is "flexible". With strict channel priority, packages in lower priority channels are not considered if a package with the same name appears in a higher priority channel. With flexible channel priority, the solver may reach into lower priority channels to fulfill dependencies, rather than raising an unsatisfiable error. With channel priority disabled, package version takes precedence, and the configured priority of channels is used only to break ties. In previous versions of conda, this parameter was configured as either "true" or "false". "true" is now an alias to "flexible". See https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-channels.html#strict-channel-priority for more information.
    channel-priority: # optional, default is 
    # Conda configuration. Comma separated list of channels to use in order of priority. See https://docs.conda.io/projects/conda/en/latest/user-guide/configuration/ for more information.
    channels: # optional, default is 
    # Conda configuration. Show channel URLs when displaying what is going to be downloaded and in conda list. The default is "false". See https://docs.conda.io/projects/conda/en/latest/user-guide/configuration/use-condarc.html#show-channel-urls-show-channel-urls for more information.
    show-channel-urls: # optional, default is 
    # Conda configuration. Conda 4.7 introduced a new .conda package file format. .conda is a more compact and faster alternative to .tar.bz2 packages. It is thus the preferred file format to use where available. Nevertheless, it is possible to force conda to only download .tar.bz2 packages by setting the use_only_tar_bz2 boolean to "true". The default is "false". See https://docs.conda.io/projects/conda/en/latest/user-guide/configuration/use-condarc.html#force-conda-to-download-only-tar-bz2-packages-use-only-tar-bz2 for more information.
    use-only-tar-bz2: # optional, default is 
    # Advanced. Prior to runnning "conda init" all shell profiles will be removed from the runner. Default is "true".
    remove-profiles: # optional, default is true
    # Use mamba (https://github.com/QuantStack/mamba) as a faster drop-in replacement for conda installs. Disabled by default. To enable, use "*" or a "x.y" version string.
    mamba-version: # optional, default is 
    # Use mamba as soon as available (either as provided by `mamba-in-installer` or installation by `mamba-version`)
    use-mamba: # optional, default is 
    # Which conda solver plugin to use. Only applies to the `conda` client, not `mamba`. Starting with Miniconda 23.5.2 and Miniforge 23.3.1, you can choose between `classic` and `libmamba`.
    conda-solver: # optional, default is libmamba
    # Architecture of Miniconda that should be installed. This is automatically detected by the runner. If you want to override it, you can use "x64", "x86", "arm64", "aarch64", "s390x".
    architecture: # optional, default is 
    # Whether a patched environment-file (if created) should be cleaned
    clean-patched-environment-file: # optional, default is true
    # Set this option to "false" to disable running the post cleanup step of the action. Default is "true"
    run-post: # optional, default is true
          
```