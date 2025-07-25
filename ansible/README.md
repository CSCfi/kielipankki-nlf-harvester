# Setting up NLF Harvester

## Pre-requisites

- [Access to Pouta](https://docs.csc.fi/accounts/how-to-add-service-access-for-project/)
- Your cPouta project's [OpenStack RC file](https://docs.csc.fi/cloud/pouta/install-client/#configure-your-terminal-environment-for-openstack)
- Key pair for cPouta instances. Created in https://pouta.csc.fi/ (Project > Compute > Key Pairs) and must be named "kielipouta".
- A fresh copy of [`Kielipankki-passwords`](https://github.com/CSCfi/Kielipankki-passwords) repository to access the needed secrets for Airflow and Puhti, synced with your passwordstore.


## Install requirements
For Python requirements, it is recommended to use a virtual environment:
```
virtualenv .venv -p python3
source .venv/bin/activate
pip install -r requirements_dev.txt
```

The activation step must be done separately for each new session.

After that, external ansible roles can be installed via
```
ansible-galaxy install -r requirements.yml
```

## Source your cPouta (OpenStack) auth file.

The [OpenStack auth file](https://docs.csc.fi/#cloud/pouta/install-client/#configure-your-terminal-environment-for-openstack) is necessary for provisioning the OpenStack resources.

```
$ source project_2006633-openrc.sh
```

## Run ansible playbook

```
ansible-playbook -i inventories/dev harvesterPouta.yml --extra-vars initial_download=true
```

Adjust `initial_download` as needed (default is false for safe reprovisioning of the production environment). This can also be edited in the Airflow web GUI under variables. NB: The variable will not automatically flip to `false` when a collection is downloaded, allowing e.g. repeated full download testing or downloading different small collections from dev instances.

Have your `kielipouta` password and `Kielipankki-passwords` GPG key password at hand, they may need to be inputted during provisioning.

### Update DAGs only

If you only wish to update the DAG files and their dependencies instead of a
full provisioning, you can run
```
ansible-playbook harvesterPouta.yml -i inventories/dev --tags dag-update
```
If the dependencies do not need to be updated, an even lighter
`minimal-dag-update` tag is available.


If you have a specific branch/tag/SHA-1 you wish to use, you can provide that:

```
ansible-playbook harvesterPouta.yml -i inventories/dev --tags dag-update --extra-vars "harvester_branch=[KP-yourbranch]"
```

## Creating multiple dev instances

If more than one development instance is in use simultaneously, they must be configured so that they won't overwrite each other's data in Puhti. This involves setting the following variables to non-default values (see production and dev inventories for one way of doing this):
- `pipeline_output_dir`
- `pipeline_tmpdir_root`
- `pipeline_extra_bin_dir`
- `restic_repository_bucket`


## Resetting the dev environment

If you need to get a fresh state for some testing, you can remove the data from Puhti and Allas by running
```
ansible-playbook reset.yml -i inventories/dev
```

NB: there are some guardrails in place to prevent accidentally deleting production data, but as this is a destructive operation, this should be handled with care.


### Publishing a Restic repository

If a newly created repository needs to be made accessible without AWS access key and key id (e.g. for demo or testing purposes), it needs to be published. This can be done in [Pouta web interface](https://pouta.csc.fi) by under _Object storage_ > _Containers_ by choosing the correct bucket and ticking the box _Public access_. This allows accessing the repository e.g. from a local laptop that has Restic installed.

Allowing truly public read-only access would also require manually adding a second, shareable, password for the repo using `restic key` command.
