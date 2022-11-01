## Run a simple download DAG in Airflow locally in Docker

Download METS file for binding 379973 and all ALTO files listed in the downloaded METS, either to a remote or local file system.

---

### Requirements

- Docker compose and Docker engine installed
- Established SSH connection to Puhti

### Instructions

1. Open a terminal and navigate to `airflow/` folder

2. Add user ID to `.env` with the following command:

    ```echo "AIRFLOW_UID=$(id -u)" > .env```

3. In order to run the Puhti dag, also add the absolute path to your `.ssh` folder to the `.env` with the following command:

    ```echo "SSH_PATH=/your/path/.ssh" >> .env```

4. Initialize Airflow in docker:

    ```docker compose up airflow-init```

5. Build image and run container:

    ```docker compose up --build```

6. Wait for the container to launch, and open Airflow in `http://0.0.0.0:8080/`

7. Login with the created default username `airflow` and password `airflow`

8. In order for the `download_altos_for_binding_to_puhti` DAG to function, you need to a SSH connection to Puhti in Admin -> Connections:

    - Select "Add a new record" from the **+** sign
    - Set Connection Id to `puhti_conn`
    - Set Connection Type to `SSH`
    - Set Host to `puhti.csc.fi`
    - Set your Puhti username as Username
    - Set the password to your private SSH key as Password
    - In the Extra-field, add the absolute path to your private SSH key (of which pair is in Puhti) in the following manner:   
        ```{"key_file": "/your/path/.ssh/id_rsa"}```


8. Turn on one or both of the DAGS `download_altos_for_binding` and `download_altos_for_binding_to_puhti` and watch the green dots appear

9. You can trigger the execution of the DAG again by pressing the "play" button on the right side of the DAG UI and selecting "Trigger DAG"

&nbsp;

`./downloads/mets` should now have one METS file and `downloads/[BINDING_ID]/alto` four ALTO files identified from the downloaded METS either locally or in Puhti `/scratch/project_2006633/nlf-harvester/` depending on the DAG you ran.
