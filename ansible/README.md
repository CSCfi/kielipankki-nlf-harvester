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
ansible-playbook -i inventories/dev harvesterPouta.yml
```

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
