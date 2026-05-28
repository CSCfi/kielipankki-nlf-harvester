## Run download DAGs in Airflow locally in Docker

---

### Requirements

- Docker compose and Docker engine installed
- Established SSH connection to the HPC environment

### Instructions

1. Open a terminal and navigate to `airflow/` folder

2. Add user ID to `.env` with the following command:

    ```echo "AIRFLOW_UID=$(id -u)" > .env```

3. In order to run the DAG, also add the absolute path to your `.ssh` folder to the `.env` with the following command:

    ```echo "SSH_PATH=/your/path/.ssh" >> .env```

4. Initialize Airflow in docker:

    ```docker compose up airflow-init```

5. Build image and run container:

    ```docker compose up --build```

6. Wait for the container to launch, and open Airflow in `http://0.0.0.0:8080/`

7. Login with the created default username `airflow` and password `airflow`

8. In order for the `subset_download_col-NNN` DAGs to function, you need to a SSH connection to the HPC environment in Admin -> Connections:

    - Select "Add a new record" from the **+** sign
    - Set Connection Id to `hpc_conn`
    - Set Connection Type to `SSH`
    - Set Host (e.g. `roihu-cpu.csc.fi`)
    - Set the username as Username
    - Set the password to the private SSH key as Password
    - In the Extra-field, add the absolute path to the private SSH key in the following manner:
        ```{"key_file": "/your/path/.ssh/id_rsa"}```


8. Turn on one of the DAGS and watch the green dots appear

9. You can trigger the execution of the DAG again by pressing the "play" button on the right side of the DAG UI and selecting "Trigger DAG"
